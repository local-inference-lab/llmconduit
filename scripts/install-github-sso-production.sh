#!/usr/bin/env bash
set -Eeuo pipefail

# Install llmconduit as a persistent user service behind an HTTPS reverse proxy.
# Secrets may be supplied as environment variables or entered at the prompts.
# Refuses to run unless the caller confirms the production host.
#
# Small hosts (for example a 2 GB / 2 vCPU VM) should NOT compile the release
# binary themselves: the optimized link alone can exhaust memory. Build on a
# larger machine with the same CPU architecture and an equal-or-older glibc:
#
#   LLMCONDUIT_BUILD_DASHBOARD=1 cargo build --locked --release
#   scp target/release/llmconduit prod-host:/tmp/llmconduit
#
# then install it here with `--binary /tmp/llmconduit` (or `--binary-url URL
# --binary-sha256 HEX`). Without either, the script builds locally with a
# low-memory profile (no LTO, 16 codegen units, one job), which still peaks
# around 2.1 GB, so it refuses on hosts with under 3 GiB of RAM+swap unless
# `--build-on-host` is passed.

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

CONFIG_PATH="${LLMCONDUIT_CONFIG_PATH:-${HOME}/.config/llmconduit/config.yaml}"
ENV_FILE="${LLMCONDUIT_ENV_FILE:-${HOME}/.config/llmconduit/dashboard.env}"
TUNING_FILE="${LLMCONDUIT_TUNING_FILE:-${HOME}/.config/llmconduit/tuning.env}"
UNIT_FILE="${HOME}/.config/systemd/user/llmconduit.service"
INSTALL_DIR="${LLMCONDUIT_INSTALL_DIR:-${HOME}/.local/lib/llmconduit}"
BINARY_PATH="${LLMCONDUIT_BINARY_PATH:-}"
BINARY_URL="${LLMCONDUIT_BINARY_URL:-}"
BINARY_SHA256="${LLMCONDUIT_BINARY_SHA256:-}"
BUILD_ON_HOST="${LLMCONDUIT_BUILD_ON_HOST:-0}"
# The low-memory local build peaks around 2.1 GB of RSS.
readonly MIN_BUILD_MEMORY_KIB=$((3 * 1024 * 1024))
# systemd memory guardrails sized for a 2 GB host: reclaim pressure starts at
# MemoryHigh, and the kernel OOM-kills only this service at MemoryMax.
MEMORY_HIGH="${LLMCONDUIT_MEMORY_HIGH:-1200M}"
MEMORY_MAX="${LLMCONDUIT_MEMORY_MAX:-1536M}"
DRY_RUN=0
CONFIRM_PRODUCTION_HOST="${LLMCONDUIT_CONFIRM_PRODUCTION_HOST:-}"
BIND_ADDR="${LLMCONDUIT_BIND_ADDR:-127.0.0.1:4000}"
HEALTH_URL="${LLMCONDUIT_HEALTH_URL:-http://127.0.0.1:4000}"
PUBLIC_ORIGIN="${LLMCONDUIT_DASHBOARD_PUBLIC_ORIGIN:-}"
GITHUB_CLIENT_ID="${LLMCONDUIT_GITHUB_CLIENT_ID:-}"
GITHUB_CLIENT_SECRET="${LLMCONDUIT_GITHUB_CLIENT_SECRET:-}"
GITHUB_ALLOWED_USERS="${LLMCONDUIT_GITHUB_ALLOWED_USERS:-}"
GITHUB_ADMIN_USERS="${LLMCONDUIT_GITHUB_ADMIN_USERS:-}"
ALLOW_MUTATIONS="${LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS:-0}"
AUTH_PEPPER="${LLMCONDUIT_AUTH_PEPPER:-}"
AUTH_BOOTSTRAP_KEY="${LLMCONDUIT_AUTH_BOOTSTRAP_KEY:-}"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"; }

usage() {
  cat <<'EOF'
Usage: install-github-sso-production.sh --confirm-production-host HOST [--dry-run]
         [--binary PATH | --binary-url URL --binary-sha256 HEX]

Requires an existing config with control_plane.storage and writes GitHub
secrets only to the separate EnvironmentFile.

  --binary PATH        Install a prebuilt llmconduit binary instead of compiling.
  --binary-url URL     Download a prebuilt binary (requires --binary-sha256).
  --binary-sha256 HEX  Expected SHA-256 of the prebuilt binary.
  --build-on-host      Compile here even when RAM+swap is under 3 GiB.

Prebuilt binaries must be built with LLMCONDUIT_BUILD_DASHBOARD=1 on a host
with the same architecture and an equal-or-older glibc. Without one, the
release binary is compiled here with a low-memory profile.

Memory tuning defaults live in the unit; override them in
~/.config/llmconduit/tuning.env (LLMCONDUIT_TUNING_FILE). Unit memory limits:
LLMCONDUIT_MEMORY_HIGH (default 1200M) and LLMCONDUIT_MEMORY_MAX (default 1536M).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --confirm-production-host)
      [[ $# -ge 2 ]] || die '--confirm-production-host requires a hostname'
      CONFIRM_PRODUCTION_HOST="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --binary)
      [[ $# -ge 2 ]] || die '--binary requires a path'
      BINARY_PATH="$2"
      shift 2
      ;;
    --binary-url)
      [[ $# -ge 2 ]] || die '--binary-url requires a URL'
      BINARY_URL="$2"
      shift 2
      ;;
    --binary-sha256)
      [[ $# -ge 2 ]] || die '--binary-sha256 requires a hex digest'
      BINARY_SHA256="$2"
      shift 2
      ;;
    --build-on-host)
      BUILD_ON_HOST=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

short_host="$(hostname)"
current_host="$(hostname -f 2>/dev/null || printf '%s' "${short_host}")"
[[ -n "${CONFIRM_PRODUCTION_HOST}" ]] || die 'pass --confirm-production-host HOST to acknowledge this production target'
[[ "${CONFIRM_PRODUCTION_HOST}" == "${current_host}" || "${CONFIRM_PRODUCTION_HOST}" == "${short_host}" ]] || \
  die 'confirmation host does not match this machine'

prompt() {
  local variable_name="$1" label="$2" value
  [[ -n "${!variable_name}" ]] && return
  read -r -p "${label}: " value
  printf -v "$variable_name" '%s' "$value"
}

prompt_secret() {
  local variable_name="$1" label="$2" value
  [[ -n "${!variable_name}" ]] && return
  read -r -s -p "${label}: " value
  printf '\n'
  printf -v "$variable_name" '%s' "$value"
}

write_env() {
  local name="$1" value="$2"
  [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] || die "${name} may not contain newlines"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '%s="%s"\n' "$name" "$value"
}

[[ -z "${BINARY_PATH}" || -z "${BINARY_URL}" ]] || die 'pass either --binary or --binary-url, not both'
if [[ -n "${BINARY_URL}" ]]; then
  [[ "${BINARY_URL}" =~ ^https:// ]] || die '--binary-url must be an https:// URL'
  [[ "${BINARY_SHA256}" =~ ^[0-9A-Fa-f]{64}$ ]] || die '--binary-url requires --binary-sha256 with a 64-hex-digit digest'
fi
if [[ -n "${BINARY_PATH}" ]]; then
  [[ -f "${BINARY_PATH}" && -x "${BINARY_PATH}" ]] || die "prebuilt binary is not an executable file: ${BINARY_PATH}"
  [[ -z "${BINARY_SHA256}" || "${BINARY_SHA256}" =~ ^[0-9A-Fa-f]{64}$ ]] || die '--binary-sha256 must be a 64-hex-digit digest'
fi
if [[ -z "${BINARY_PATH}" && -z "${BINARY_URL}" ]]; then
  if [[ "${BUILD_ON_HOST}" != 1 && -r /proc/meminfo ]]; then
    build_memory_kib="$(awk '/^(MemTotal|SwapTotal):/ { total += $2 } END { print total + 0 }' /proc/meminfo)"
    if (( build_memory_kib < MIN_BUILD_MEMORY_KIB )); then
      die "this host has $((build_memory_kib / 1024)) MiB of RAM+swap; compiling needs ~2.1 GB. Build elsewhere and pass --binary PATH (or --binary-url URL --binary-sha256 HEX), add swap, or pass --build-on-host"
    fi
  fi
  need cargo
  need npm
fi
need curl
need openssl
need sha256sum
need systemctl
[[ -f "${CONFIG_PATH}" ]] || die "config file not found: ${CONFIG_PATH}"
if ! grep -Eq '^[[:space:]]*control_plane:[[:space:]]*($|#)' "${CONFIG_PATH}" || \
   ! grep -Eq '^[[:space:]]*storage:[[:space:]]*($|[{])' "${CONFIG_PATH}"; then
  die 'config must include control_plane.storage (sqlite or postgres) for GitHub SSO users and key grants'
fi

prompt PUBLIC_ORIGIN 'Public HTTPS origin (for example https://llm.example.com)'
prompt GITHUB_CLIENT_ID 'GitHub OAuth client ID'
prompt_secret GITHUB_CLIENT_SECRET 'GitHub OAuth client secret'
prompt GITHUB_ALLOWED_USERS 'Allowed GitHub logins (comma-separated)'
prompt GITHUB_ADMIN_USERS 'Administrator GitHub logins (comma-separated)'

[[ "${PUBLIC_ORIGIN}" =~ ^https://(\[[0-9A-Fa-f:]+\]|[A-Za-z0-9.-]+)(:[0-9]{1,5})?$ ]] || \
  die 'public origin must be an HTTPS origin with no path, query, fragment, or trailing slash'
[[ "${GITHUB_CLIENT_ID}" =~ ^[A-Za-z0-9._-]+$ ]] || die 'GitHub client ID contains invalid characters'
[[ -n "${GITHUB_CLIENT_SECRET}" ]] || die 'GitHub client secret is required'
[[ "${GITHUB_ALLOWED_USERS}" =~ ^[A-Za-z0-9._-]+([[:space:]]*,[[:space:]]*[A-Za-z0-9._-]+)*$ ]] || \
  die 'allowed users must be a comma-separated list of GitHub logins'
if [[ ! "${GITHUB_ADMIN_USERS}" =~ ^[A-Za-z0-9._-]+([[:space:]]*,[[:space:]]*[A-Za-z0-9._-]+)*$ ]]; then
  die 'admin users must be a comma-separated list of GitHub logins'
fi

allowed_users=",${GITHUB_ALLOWED_USERS//[[:space:]]/},"
allowed_users="${allowed_users,,}"
admin_users="${GITHUB_ADMIN_USERS//[[:space:]]/}"
IFS=',' read -r -a admin_logins <<<"${admin_users,,}"
for admin_login in "${admin_logins[@]}"; do
  [[ "${allowed_users}" == *",${admin_login},"* ]] || \
    die "administrator '${admin_login}' is not present in the allowed-users list"
done
[[ "${ALLOW_MUTATIONS}" =~ ^(0|1|true|false|yes|no)$ ]] || \
  die 'LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS must be 0/1, true/false, or yes/no'

printf '\nGitHub callback URL (must exactly match the registered callback):\n  %s/dashboard/auth/github/callback\n\n' "${PUBLIC_ORIGIN}"

if [[ "${DRY_RUN}" != 1 ]]; then
  mkdir -p -- "$(dirname -- "${ENV_FILE}")" "$(dirname -- "${UNIT_FILE}")"
  chmod 700 -- "$(dirname -- "${ENV_FILE}")"
fi

SESSION_KEY=''
if [[ -f "${ENV_FILE}" ]]; then
  SESSION_KEY="$(sed -n 's/^LLMCONDUIT_DASHBOARD_SESSION_KEY="\(.*\)"$/\1/p' "${ENV_FILE}" | head -n 1)"
  [[ -n "${AUTH_PEPPER}" ]] || \
    AUTH_PEPPER="$(sed -n 's/^LLMCONDUIT_AUTH_PEPPER="\(.*\)"$/\1/p' "${ENV_FILE}" | head -n 1)"
  [[ -n "${AUTH_BOOTSTRAP_KEY}" ]] || \
    AUTH_BOOTSTRAP_KEY="$(sed -n 's/^LLMCONDUIT_AUTH_BOOTSTRAP_KEY="\(.*\)"$/\1/p' "${ENV_FILE}" | head -n 1)"
  if [[ "${DRY_RUN}" != 1 ]]; then
    backup_file="${ENV_FILE}.backup.$(date -u +%Y%m%dT%H%M%SZ)"
    cp -p -- "${ENV_FILE}" "${backup_file}"
    chmod 600 -- "${backup_file}"
    mapfile -d '' -t secret_backups < <(
      find "$(dirname -- "${ENV_FILE}")" -maxdepth 1 -type f \
        -name "$(basename -- "${ENV_FILE}").backup.*" -printf '%T@ %p\0' | sort -zrn
    )
    for backup_entry in "${secret_backups[@]:3}"; do
      rm -f -- "${backup_entry#* }"
    done
  fi
fi
[[ -n "${SESSION_KEY}" ]] || SESSION_KEY="$(openssl rand -base64 48 | tr -d '\n')"
[[ -n "${AUTH_PEPPER}" ]] || AUTH_PEPPER="$(openssl rand -base64 48 | tr -d '\n')"

if [[ "${DRY_RUN}" == 1 ]]; then
  printf '[dry-run] would write secrets EnvironmentFile: %s\n' "${ENV_FILE}"
else
  ENV_TMP="$(mktemp "${ENV_FILE}.tmp.XXXXXX")"
  chmod 600 -- "${ENV_TMP}"
  {
    write_env LLMCONDUIT_BIND_ADDR "${BIND_ADDR}"
    write_env LLMCONDUIT_DASHBOARD_PUBLIC_ORIGIN "${PUBLIC_ORIGIN}"
    write_env LLMCONDUIT_DASHBOARD_SESSION_KEY "${SESSION_KEY}"
    write_env LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS "${ALLOW_MUTATIONS}"
    write_env LLMCONDUIT_GITHUB_CLIENT_ID "${GITHUB_CLIENT_ID}"
    write_env LLMCONDUIT_GITHUB_CLIENT_SECRET "${GITHUB_CLIENT_SECRET}"
    write_env LLMCONDUIT_GITHUB_ALLOWED_USERS "${GITHUB_ALLOWED_USERS}"
    write_env LLMCONDUIT_GITHUB_ADMIN_USERS "${GITHUB_ADMIN_USERS}"
    write_env LLMCONDUIT_AUTH_PEPPER "${AUTH_PEPPER}"
    [[ -z "${AUTH_BOOTSTRAP_KEY}" ]] || write_env LLMCONDUIT_AUTH_BOOTSTRAP_KEY "${AUTH_BOOTSTRAP_KEY}"
    [[ -z "${LLMCONDUIT_FLEET_URL:-}" ]] || write_env LLMCONDUIT_FLEET_URL "${LLMCONDUIT_FLEET_URL}"
    [[ -z "${LLMCONDUIT_FLEET_TOKEN_FILE:-}" ]] || write_env LLMCONDUIT_FLEET_TOKEN_FILE "${LLMCONDUIT_FLEET_TOKEN_FILE}"
  } >"${ENV_TMP}"
  mv -f -- "${ENV_TMP}" "${ENV_FILE}"
  chmod 600 -- "${ENV_FILE}"
fi
unset GITHUB_CLIENT_SECRET SESSION_KEY AUTH_PEPPER AUTH_BOOTSTRAP_KEY

BINARY="${INSTALL_DIR}/llmconduit"

# Install `$1` as the service binary atomically, verifying `$2` (optional).
install_binary() {
  local source="$1" expected_sha="${2:-}" actual_sha tmp
  if [[ -n "${expected_sha}" ]]; then
    actual_sha="$(sha256sum -- "${source}" | cut -d' ' -f1)"
    [[ "${actual_sha,,}" == "${expected_sha,,}" ]] || \
      die "binary SHA-256 mismatch: expected ${expected_sha}, got ${actual_sha}"
  fi
  "${source}" --help >/dev/null 2>&1 || die "binary does not run on this host: ${source}"
  mkdir -p -- "${INSTALL_DIR}"
  tmp="$(mktemp "${BINARY}.tmp.XXXXXX")"
  cp -- "${source}" "${tmp}"
  chmod 755 -- "${tmp}"
  mv -f -- "${tmp}" "${BINARY}"
}

if [[ -n "${BINARY_URL}" ]]; then
  printf 'Downloading the prebuilt release binary...\n'
  if [[ "${DRY_RUN}" == 1 ]]; then
    printf '[dry-run] would download %s, verify its SHA-256, and install %s\n' "${BINARY_URL}" "${BINARY}"
  else
    download="$(mktemp)"
    trap 'rm -f -- "${download}"' EXIT
    curl --fail --silent --show-error --location --proto '=https' --output "${download}" "${BINARY_URL}"
    chmod 700 -- "${download}"
    install_binary "${download}" "${BINARY_SHA256}"
  fi
elif [[ -n "${BINARY_PATH}" ]]; then
  if [[ "${DRY_RUN}" == 1 ]]; then
    printf '[dry-run] would install prebuilt binary %s as %s\n' "${BINARY_PATH}" "${BINARY}"
  else
    install_binary "${BINARY_PATH}" "${BINARY_SHA256}"
  fi
else
  printf 'Building the release binary with the dashboard embedded (low-memory profile)...\n'
  printf 'Tip: on a small host, build elsewhere and pass --binary instead.\n'
  if [[ "${DRY_RUN}" == 1 ]]; then
    printf '[dry-run] would run cargo build --locked --release with embedded dashboard and install %s\n' "${BINARY}"
  else
    # Thin LTO + one codegen unit (the release profile) can OOM a 2 GB host at
    # link time; trade a little runtime speed for a build that fits. Callers may
    # still override any of these.
    (cd -- "${REPO_DIR}" && \
      LLMCONDUIT_BUILD_DASHBOARD=1 \
      CARGO_PROFILE_RELEASE_LTO="${CARGO_PROFILE_RELEASE_LTO:-false}" \
      CARGO_PROFILE_RELEASE_CODEGEN_UNITS="${CARGO_PROFILE_RELEASE_CODEGEN_UNITS:-16}" \
      CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}" \
      cargo build --locked --release)
    install_binary "${REPO_DIR}/target/release/llmconduit"
  fi
fi

if [[ "${DRY_RUN}" == 1 ]]; then
  printf '[dry-run] would create tuning EnvironmentFile if absent: %s\n' "${TUNING_FILE}"
elif [[ ! -e "${TUNING_FILE}" ]]; then
  mkdir -p -- "$(dirname -- "${TUNING_FILE}")"
  cat >"${TUNING_FILE}" <<'EOF'
# llmconduit memory/CPU tuning overrides (not secrets). The unit already sets
# small-host defaults; uncomment a line here to override it, then run
#   systemctl --user restart llmconduit.service
#LLMCONDUIT_DASHBOARD_SNAPSHOT_BYTES=33554432
#LLMCONDUIT_MONITOR_PAYLOAD_PREVIEW_BYTES=16777216
#LLMCONDUIT_INFLIGHT_REQUEST_BODY_BYTES=67108864
#LLMCONDUIT_PERSISTENCE_QUEUE_MAX_BYTES=16777216
#LLMCONDUIT_PROVIDER_METRICS_INTERVAL_SECS=120
#LLMCONDUIT_MAX_BLOCKING_THREADS=16
#MALLOC_ARENA_MAX=2
EOF
  chmod 600 -- "${TUNING_FILE}"
fi

if [[ "${DRY_RUN}" == 1 ]]; then
  printf '[dry-run] would write unit file: %s\n' "${UNIT_FILE}"
else
  UNIT_TMP="$(mktemp "${UNIT_FILE}.tmp.XXXXXX")"
  cat >"${UNIT_TMP}" <<EOF
[Unit]
Description=llmconduit production gateway and dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${REPO_DIR}
# Small-host memory defaults (2 GB VM). Values in the tuning file override them;
# the secrets file comes last.
Environment=MALLOC_ARENA_MAX=2
Environment=LLMCONDUIT_DASHBOARD_SNAPSHOT_BYTES=33554432
Environment=LLMCONDUIT_MONITOR_PAYLOAD_PREVIEW_BYTES=16777216
Environment=LLMCONDUIT_INFLIGHT_REQUEST_BODY_BYTES=67108864
Environment=LLMCONDUIT_PERSISTENCE_QUEUE_MAX_BYTES=16777216
Environment=LLMCONDUIT_PROVIDER_METRICS_INTERVAL_SECS=120
EnvironmentFile=-${TUNING_FILE}
EnvironmentFile=${ENV_FILE}
ExecStart=${BINARY} start --config ${CONFIG_PATH} --with-debug-ui
Restart=on-failure
RestartSec=3
MemoryHigh=${MEMORY_HIGH}
MemoryMax=${MEMORY_MAX}
UMask=0077
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=default.target
EOF
  chmod 644 -- "${UNIT_TMP}"
  mv -f -- "${UNIT_TMP}" "${UNIT_FILE}"
fi

if [[ "${DRY_RUN}" == 1 ]]; then
  printf '[dry-run] would reload and start llmconduit.service\n'
else
  systemctl --user stop llmconduit-preview.service 2>/dev/null || true
  systemctl --user daemon-reload
  systemctl --user enable --now llmconduit.service
fi

printf 'Waiting for the dashboard...\n'
if [[ "${DRY_RUN}" == 1 ]]; then
  printf '[dry-run] would probe %s/dashboard and GitHub start redirect\n' "${HEALTH_URL}"
else
  curl --fail --silent --show-error --retry 20 --retry-connrefused --retry-delay 1 \
    --output /dev/null "${HEALTH_URL}/dashboard"
  github_status="$(curl --silent --output /dev/null --write-out '%{http_code}' "${HEALTH_URL}/dashboard/auth/github/start")"
  [[ "${github_status}" == '303' ]] || die "GitHub login start returned HTTP ${github_status}, expected 303"
fi

if [[ "${DRY_RUN}" == 1 ]]; then
  printf '\nProduction configuration validated; no files or services were changed.\n'
else
  printf '\nInstalled successfully.\n'
fi
printf '  Service:       llmconduit.service\n'
printf '  Local health:  %s/dashboard\n' "${HEALTH_URL}"
printf '  Public URL:    %s/dashboard\n' "${PUBLIC_ORIGIN}"
printf '  Secrets file:  %s (mode 0600)\n' "${ENV_FILE}"
printf '  Tuning file:   %s\n' "${TUNING_FILE}"
printf '  Binary:        %s\n' "${BINARY}"
printf '  Logs:          journalctl --user -u llmconduit.service -f\n'
if [[ "${ALLOW_MUTATIONS}" =~ ^(0|false|no)$ ]]; then
  printf '\nDashboard mutations remain disabled. Re-run with LLMCONDUIT_DASHBOARD_ALLOW_MUTATIONS=1 to enable Access/Fleet writes.\n'
fi
if command -v loginctl >/dev/null 2>&1; then
  linger="$(loginctl show-user "$(id -un)" --property=Linger --value 2>/dev/null || true)"
  if [[ "${linger}" != 'yes' ]]; then
    printf '\nBoot persistence: run `sudo loginctl enable-linger %s` if this host must start the user service before login.\n' "$(id -un)"
  fi
fi
