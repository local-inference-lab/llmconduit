//! Age-based cleanup of debug / request-log dump files.
//!
//! The upstream request log (`upstream_request_log_path`) is append-only JSONL
//! with no rotation, so the directory holding dump files can grow unbounded.
//! This module deletes eligible dump files older than a configurable max age.
//!
//! F1f (durable turn capture) reuses the SAME age window for two surfaces under a
//! `turn_capture_dir`: [`cleanup_dump_files`] prunes the published
//! `<api_call_id>.json` artifacts (top-level `*.json`), and
//! [`cleanup_orphan_work_dirs`] sweeps orphaned per-turn `.work/<id>/` section dirs
//! left behind when a process crashes before `TurnCaptureState::finalize_and_assemble`
//! could delete them. Both run in the same [`spawn_cleanup`] pass, but with DISTINCT
//! scopes — the artifact/dump prune spans every `Config::debug_log_dirs()` entry, while
//! the destructive `.work` sweep is scoped to `turn_capture_dir` ALONE (turn capture is
//! the sole creator of `.work/<id>/` subdirs, and only under `turn_capture_dir`, so the
//! sweep must never run over a request-log dir — F1f review r1). Neither reclaims an
//! in-flight turn (its section files carry fresh mtimes, so it is younger than the
//! window — and the production caller only runs cleanup at startup, before any turn is
//! registered).
//!
//! The core [`cleanup_dump_files`] / [`cleanup_orphan_work_dirs`] functions are
//! synchronous and take an injected `now`, so tests can age entries out by passing a
//! cutoff rather than backdating mtimes. Callers on the tokio runtime must invoke them
//! via `spawn_blocking` (see [`spawn_cleanup`]) because they perform blocking
//! filesystem IO.

use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::SystemTime;

/// File extensions eligible for cleanup. Other extensions (and extensionless
/// files) are skipped so unrelated files in the directory are never touched.
const ELIGIBLE_EXTENSIONS: [&str; 2] = ["json", "ndjson"];

/// Delete eligible dump files in `dir` whose modification time is older than
/// `max_age` relative to `now`. Returns the number of files actually deleted.
///
/// Behavior:
/// - Only top-level `*.json` / `*.ndjson` files are considered; other
///   extensions are skipped and subdirectories are ignored (never removed).
/// - A missing directory returns `0` with no error (cleanup is idempotent).
/// - A per-file metadata or removal error (e.g. a concurrent deletion race) is
///   tolerated: that file is skipped, the count is not incremented for it, and
///   cleanup continues with the remaining entries.
///
/// This is blocking IO; do not call it directly on the async runtime. Use
/// [`spawn_cleanup`] from async contexts.
pub fn cleanup_dump_files(dir: &Path, max_age: Duration, now: SystemTime) -> usize {
    cleanup_dump_files_with_remover(dir, max_age, now, |path| fs::remove_file(path))
}

/// Delete one explicitly configured request-log file when it is stale. Unlike
/// directory sweeping this can safely handle `.jsonl` without touching an
/// operator's unrelated JSON files in the same directory.
pub fn cleanup_exact_file(path: &Path, max_age: Duration, now: SystemTime) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    metadata.is_file()
        && is_older_than_cutoff(&metadata, now.checked_sub(max_age))
        && fs::remove_file(path).is_ok()
}

/// Delete only published turn-capture artifacts owned by llmconduit. HTTP
/// capture ids are minted with the `api_` prefix; other JSON files are left
/// alone even when an operator shares the directory accidentally.
pub fn cleanup_turn_capture_artifacts(
    capture_dir: &Path,
    max_age: Duration,
    now: SystemTime,
) -> usize {
    let Ok(entries) = fs::read_dir(capture_dir) else {
        return 0;
    };
    let cutoff = now.checked_sub(max_age);
    entries
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("api_") && name.ends_with(".json"))
        })
        .filter(|entry| {
            entry
                .metadata()
                .is_ok_and(|metadata| metadata.is_file() && is_older_than_cutoff(&metadata, cutoff))
        })
        .filter(|entry| fs::remove_file(entry.path()).is_ok())
        .count()
}

/// [`cleanup_dump_files`] with the per-file removal injected, so a test can drive
/// the removal-error-tolerance path deterministically (force `Err` for one
/// eligible file and confirm cleanup still removes the rest, counting only the
/// successes) without racing the filesystem. Production always passes
/// [`fs::remove_file`]; this is the same `now`-injection seam the rest of the
/// module already relies on, extended to the removal call.
pub fn cleanup_dump_files_with_remover(
    dir: &Path,
    max_age: Duration,
    now: SystemTime,
    mut remove: impl FnMut(&Path) -> std::io::Result<()>,
) -> usize {
    // Missing directory (or a path that isn't a directory) -> nothing to do.
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return 0,
    };

    let cutoff = now.checked_sub(max_age);

    let mut deleted = 0usize;
    for entry in entries {
        // A failure to read a single dir entry is tolerated like a removal race.
        let Ok(entry) = entry else {
            continue;
        };
        let path = entry.path();

        if !is_eligible_extension(&path) {
            continue;
        }

        // Use the entry's own file-type when available to avoid an extra stat;
        // fall back to metadata. Either way, only regular files are eligible so
        // subdirectories (even with an eligible-looking name) are ignored.
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        if !metadata.is_file() {
            continue;
        }

        if is_older_than_cutoff(&metadata, cutoff) {
            // Tolerate races: only count a deletion that actually succeeded.
            match remove(&path) {
                Ok(()) => deleted += 1,
                Err(error) => {
                    tracing::debug!(
                        path = %path.display(),
                        %error,
                        "skipping debug dump file that could not be removed"
                    );
                }
            }
        }
    }

    deleted
}

/// F1f (durable turn capture): reclaim orphaned per-turn `.work/<id>/` section dirs
/// under `<capture_dir>/.work/` that a crash left behind (the process died before
/// `TurnCaptureState::finalize_and_assemble` could delete the dir). Returns the number
/// of orphan dirs actually removed.
///
/// Mirrors [`cleanup_dump_files`]: the SAME injected-`now` seam, the SAME age policy
/// (`now - max_age` cutoff), and the SAME best-effort per-entry tolerance (a removal
/// race is logged and skipped, not fatal). Only a `.work/<id>/` whose MOST-RECENT
/// activity — the newest mtime among the dir itself and its immediate section files —
/// predates the cutoff is removed, so a turn still streaming to its section files
/// (fresh file mtimes) is NEVER swept even if its dir mtime aged. In-flight protection
/// is therefore mtime-based; the production caller ([`spawn_cleanup`]) additionally
/// only runs cleanup at startup, before any turn is registered, so a live turn cannot
/// coincide with a sweep in practice.
///
/// A missing `<capture_dir>/.work/` (the common case — no crash residue) returns `0`
/// with no error. The production caller ([`spawn_cleanup`]) runs this over
/// `turn_capture_dir` ALONE — NEVER the request-log dirs — because turn capture is the
/// sole creator of `.work/<api_call_id>/` subdirs and only under `turn_capture_dir`;
/// sweeping a request-log dir could `remove_dir_all` an unrelated `.work/` subtree it
/// does not own (F1f review r1, HIGH). This is blocking IO; do not call it directly on
/// the async runtime.
pub fn cleanup_orphan_work_dirs(capture_dir: &Path, max_age: Duration, now: SystemTime) -> usize {
    let work_root = capture_dir.join(".work");
    // Missing `.work/` (or a path that isn't a directory) -> no crash residue.
    let entries = match fs::read_dir(&work_root) {
        Ok(entries) => entries,
        Err(_) => return 0,
    };
    // `now - max_age` underflowed (max_age beyond the epoch): nothing can be older
    // than the cutoff, so keep everything (mirrors `is_older_than_cutoff`).
    let Some(cutoff) = now.checked_sub(max_age) else {
        return 0;
    };

    let mut swept = 0usize;
    for entry in entries {
        // Tolerate a failing dir entry like a removal race.
        let Ok(entry) = entry else {
            continue;
        };
        // Only per-turn SUBDIRECTORIES are eligible; a stray file under `.work/` is
        // left untouched (never removed).
        match entry.file_type() {
            Ok(file_type) if file_type.is_dir() => {}
            _ => continue,
        }
        let path = entry.path();
        // Reclaim ONLY a work dir whose most-recent activity predates the cutoff, so
        // an in-flight turn actively writing its section files is never swept. A dir
        // with no readable mtime at all is kept (conservative).
        if newest_mtime(&path).is_some_and(|activity| activity < cutoff) {
            match fs::remove_dir_all(&path) {
                Ok(()) => swept += 1,
                Err(error) => {
                    tracing::debug!(
                        path = %path.display(),
                        %error,
                        "skipping orphan turn-capture .work dir that could not be removed"
                    );
                }
            }
        }
    }

    swept
}

/// F1 (Fable review): reclaim crash-orphaned `<api_call_id>.json.tmp` artifact temp
/// files under `capture_dir` older than the same age window. [`super`]'s
/// `TurnCaptureState::assemble_blocking` writes `<id>.json.tmp`, `fsync`s it, then
/// atomically renames it to `<id>.json`; a crash BETWEEN the write and the rename
/// leaves an artifact-sized `.tmp` that [`cleanup_dump_files`] never reclaims (its
/// extension is `tmp`, not in [`ELIGIBLE_EXTENSIONS`]). Returns the number of
/// `.json.tmp` files actually removed.
///
/// Mirrors [`cleanup_orphan_work_dirs`]: the SAME injected-`now` seam, the SAME
/// `now - max_age` cutoff (so a FRESH in-progress `<id>.json.tmp` an assembly is
/// writing RIGHT NOW is protected by its recent mtime and never deleted), and the
/// SAME best-effort per-entry tolerance. ONLY a top-level regular file whose name
/// ends in `.json.tmp` is eligible, so an unrelated `.tmp`, a published `.json`, or a
/// subdirectory is never touched. The production caller ([`spawn_cleanup`]) runs this
/// over `turn_capture_dir` ALONE — NEVER the request-log dirs — because turn capture
/// is the sole writer of `<id>.json.tmp` and only under `turn_capture_dir` (identical
/// scoping to the destructive `.work` sweep, F1f review r1). Blocking IO; do not call
/// it directly on the async runtime.
pub fn cleanup_orphan_tmp_files(capture_dir: &Path, max_age: Duration, now: SystemTime) -> usize {
    // Missing `turn_capture_dir` (or a path that isn't a directory) -> nothing to do.
    let entries = match fs::read_dir(capture_dir) {
        Ok(entries) => entries,
        Err(_) => return 0,
    };
    // `now - max_age` underflowed (max_age beyond the epoch): nothing can be older
    // than the cutoff, so keep everything (mirrors `is_older_than_cutoff`).
    let Some(cutoff) = now.checked_sub(max_age) else {
        return 0;
    };

    let mut removed = 0usize;
    for entry in entries {
        // Tolerate a failing dir entry like a removal race.
        let Ok(entry) = entry else {
            continue;
        };
        let path = entry.path();
        // ONLY the artifact temp files: a `*.json.tmp` NAME. A stray `.tmp`, a
        // published `.json`, or any other file is never targeted.
        if !path
            .file_name()
            .and_then(OsStr::to_str)
            .is_some_and(|name| name.ends_with(".json.tmp"))
        {
            continue;
        }
        // Only a regular file is eligible; a directory named `*.json.tmp` is ignored.
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        if !metadata.is_file() {
            continue;
        }
        if is_older_than_cutoff(&metadata, Some(cutoff)) {
            // Tolerate races: only count a removal that actually succeeded.
            match fs::remove_file(&path) {
                Ok(()) => removed += 1,
                Err(error) => {
                    tracing::debug!(
                        path = %path.display(),
                        %error,
                        "skipping orphan turn-capture .json.tmp that could not be removed"
                    );
                }
            }
        }
    }

    removed
}

/// The most-recent activity time for a `.work/<id>/` dir: the LATER of the dir's own
/// mtime and the newest mtime among its immediate entries (the per-section temp
/// files). Any recent write anywhere inside protects the dir from the sweep. `None`
/// when no mtime is readable at all (the caller then keeps the dir — conservative).
fn newest_mtime(dir: &Path) -> Option<SystemTime> {
    let mut newest = fs::metadata(dir).and_then(|meta| meta.modified()).ok();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(modified) = entry.metadata().and_then(|meta| meta.modified()) {
                newest = Some(newest.map_or(modified, |current| current.max(modified)));
            }
        }
    }
    newest
}

/// Run [`cleanup_dump_files`] AND [`cleanup_orphan_work_dirs`] on the blocking thread
/// pool (one pass, one `now`) and log the outcome.
///
/// Two DISTINCT surfaces with DISTINCT scopes:
/// - [`cleanup_dump_files`] age-rotates the published `<id>.json` artifacts + the
///   request-log dump files across EVERY active dump dir (`dump_dirs` =
///   `Config::debug_log_dirs()`).
/// - [`cleanup_orphan_work_dirs`] sweeps orphaned per-turn `.work/<id>/` section dirs,
///   which ONLY turn capture ever creates and ONLY under `turn_capture_dir`. The
///   destructive `remove_dir_all` sweep is therefore scoped to `turn_capture_dir`
///   ALONE — it must NEVER run over a request-log dir, where an unrelated `.work/`
///   subtree the gateway does not own could be reclaimed (F1f review r1, HIGH). A
///   `None` `turn_capture_dir` ⇒ the sweep does nothing.
///
/// Returns immediately if `max_age_hours` is `None` (the feature is opt-in) or `0`.
/// The spawned task is detached: cleanup is best-effort and must never block startup
/// or fail the server.
pub fn spawn_cleanup(
    dump_dirs: Vec<PathBuf>,
    turn_capture_dir: Option<PathBuf>,
    max_age_hours: Option<u64>,
) {
    let Some(max_age_hours) = max_age_hours else {
        return;
    };
    if max_age_hours == 0 {
        // Treat 0 as disabled rather than "delete everything immediately",
        // which would race with files being written this same second.
        return;
    }
    let max_age = Duration::from_secs(max_age_hours.saturating_mul(3600));
    tokio::task::spawn_blocking(move || {
        // One `now` for both surfaces so the artifact prune and the `.work` sweep
        // apply an identical cutoff.
        let now = SystemTime::now();
        // Age-rotate the published artifacts + request-log dump files across every
        // active dump dir.
        for dir in &dump_dirs {
            let deleted = cleanup_dump_files(dir, max_age, now);
            if deleted > 0 {
                tracing::info!(
                    deleted,
                    dir = %dir.display(),
                    "cleaned up old debug/request-log dump file(s)"
                );
            }
        }
        // F1f: reclaim orphaned per-turn `.work/<id>/` dirs (crash residue). SCOPED to
        // `turn_capture_dir` ALONE — turn capture is the sole creator of
        // `.work/<api_call_id>/` subdirs, so running this destructive sweep over a
        // request-log dir could `remove_dir_all` an unrelated `.work/` subtree it does
        // not own (F1f review r1, HIGH).
        if let Some(capture_dir) = turn_capture_dir.as_ref() {
            let swept = cleanup_orphan_work_dirs(capture_dir, max_age, now);
            if swept > 0 {
                tracing::info!(
                    swept,
                    dir = %capture_dir.display(),
                    "swept orphaned turn-capture .work dir(s)"
                );
            }
            // F1 (Fable review): also reclaim crash-orphaned `<id>.json.tmp` artifact
            // temp files (a crash between the tmp write and the atomic rename). SAME
            // scope as the `.work` sweep — `turn_capture_dir` ALONE, never a
            // request-log dir (turn capture is the sole writer of `<id>.json.tmp`).
            let tmp_removed = cleanup_orphan_tmp_files(capture_dir, max_age, now);
            if tmp_removed > 0 {
                tracing::info!(
                    removed = tmp_removed,
                    dir = %capture_dir.display(),
                    "reclaimed crash-orphaned turn-capture .json.tmp file(s)"
                );
            }
        }
    });
}

/// Run narrowly scoped startup cleanup and wait for it to finish before the
/// listener accepts traffic. Request logs are exact paths; turn-capture cleanup
/// is restricted to gateway-owned `api_*.json`/work/temp artifacts.
pub async fn cleanup_scoped(
    request_log_files: Vec<PathBuf>,
    turn_capture_dir: Option<PathBuf>,
    max_age_hours: Option<u64>,
) {
    let Some(max_age_hours) = max_age_hours.filter(|value| *value > 0) else {
        return;
    };
    let max_age = Duration::from_secs(max_age_hours.saturating_mul(3600));
    let result = tokio::task::spawn_blocking(move || {
        let now = SystemTime::now();
        let request_logs = request_log_files
            .iter()
            .filter(|path| cleanup_exact_file(path, max_age, now))
            .count();
        let mut artifacts = 0;
        let mut work_dirs = 0;
        let mut temp_files = 0;
        if let Some(capture_dir) = turn_capture_dir.as_ref() {
            artifacts = cleanup_turn_capture_artifacts(capture_dir, max_age, now);
            work_dirs = cleanup_orphan_work_dirs(capture_dir, max_age, now);
            temp_files = cleanup_orphan_tmp_files(capture_dir, max_age, now);
        }
        (request_logs, artifacts, work_dirs, temp_files)
    })
    .await;
    match result {
        Ok((request_logs, artifacts, work_dirs, temp_files)) => {
            if request_logs + artifacts + work_dirs + temp_files > 0 {
                tracing::info!(
                    request_logs,
                    artifacts,
                    work_dirs,
                    temp_files,
                    "cleaned up expired gateway-owned log artifacts"
                );
            }
        }
        Err(error) => tracing::warn!(%error, "startup log cleanup task failed"),
    }
}

fn is_eligible_extension(path: &Path) -> bool {
    path.extension()
        .and_then(OsStr::to_str)
        .map(|extension| {
            ELIGIBLE_EXTENSIONS
                .iter()
                .any(|eligible| extension.eq_ignore_ascii_case(eligible))
        })
        .unwrap_or(false)
}

fn is_older_than_cutoff(metadata: &fs::Metadata, cutoff: Option<SystemTime>) -> bool {
    // If `now - max_age` underflowed (max_age beyond the epoch), nothing can be
    // older than the cutoff, so keep everything.
    let Some(cutoff) = cutoff else {
        return false;
    };
    match metadata.modified() {
        Ok(modified) => modified < cutoff,
        // No mtime available: be conservative and keep the file.
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::cleanup_dump_files;
    use std::fs;
    use std::time::Duration;
    use std::time::SystemTime;

    // Smoke test of the core path; exhaustive behavior coverage lives in
    // tests/port_logging.rs (the 8 ported claude-relay behaviors).
    #[test]
    fn deletes_eligible_file_older_than_cutoff() {
        let dir = std::env::temp_dir().join(format!(
            "llmconduit-log-rotation-{}",
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let target = dir.join("dump.json");
        fs::write(&target, "{}\n").expect("write file");

        // Inject a `now` 10h in the future so the just-written file is older
        // than a 2h max_age without backdating its mtime.
        let now = SystemTime::now() + Duration::from_secs(10 * 3600);
        let deleted = cleanup_dump_files(&dir, Duration::from_secs(2 * 3600), now);

        assert_eq!(deleted, 1);
        assert!(!target.exists());

        let _ = fs::remove_dir_all(&dir);
    }
}
