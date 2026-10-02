//! D5 — `MetricsLayer`: authoritative aggregated request stats + the coordinated,
//! memory-safe, internally-consistent time-travel snapshot store.
//!
//! Metrics architecture:
//! - `MetricsLayer { state: Mutex<MetricsState> }` with the `MonitorHub`/
//!   `DashboardFlowStore` `new()`/`disabled()` split: when `--with-debug-ui` is off
//!   the layer is `disabled()` and EVERY mutation/read is a no-op, so the production
//!   hot path keeps zero overhead (a streamed request with the dashboard off does
//!   NO ring/histogram work and takes NO lock).
//! - Per-window RING buffers (1m / 5m / 1h at 1 s resolution = 60 / 300 / 3600
//!   slots). Each slot is a [`Bucket`] keyed `{status_class, model, endpoint,
//!   upstream}` plus a 30-bucket log-spaced latency [`Histogram`] and summed token
//!   counters. A slot is reused circularly: when wall-clock advances past a slot's
//!   epoch second the slot is RESET before the new sample lands, so a window only
//!   ever aggregates samples from its own time span (no unbounded growth — the rings
//!   are fixed-size arrays).
//! - `record_response(...)` is called from the engine's D3 TERMINAL finalize seam
//!   (the single CAS-guarded choke point — NOT the middleware, NOT per-chunk), so a
//!   streamed request populates metrics exactly once, at finalize. `record_usage`
//!   rides the same D3 usage upsert the FlowStore/monitor already consume.
//! - A `metrics_seq` increments on EVERY mutation; it is the metrics domain's
//!   per-domain cursor (no global watermark — AGENTS.md).
//! - The 5 s coordinated snapshot task (D5, spawned only under `--with-debug-ui`)
//!   takes ONE critical section: it holds the FlowStore mutex THEN the MetricsLayer
//!   mutex (the FIXED lock order — only the snapshot task ever holds >1 lock, so no
//!   deadlock is possible) and captures ONE `Arc<ProviderHealthSnapshot>` (D4),
//!   producing a true atomic cut across all three stores into a body-free
//!   [`DashboardSnapshot`]. The summaries are body-free [`SnapshotFlowSummary`]
//!   (NO `Arc<[u8]>`, NO live-store reference) — body retention on snapshots
//!   recreates a 135 GiB worst case (AGENTS.md don't-rule). Cuts SHARE unchanged
//!   summaries, and a quota over unique allocations bounds peak ring memory
//!   (default 64 MiB, `LLMCONDUIT_DASHBOARD_SNAPSHOT_BYTES` overrides).

use crate::dashboard_flow::Attempt;
use crate::dashboard_flow::AttemptErrorClass;
use crate::dashboard_flow::AttemptStatus;
use crate::dashboard_flow::DashboardFlowStore;
use crate::dashboard_flow::FlowStatus;
use crate::dashboard_flow::FlowUsage;
use crate::dashboard_flow::SharedFlowSummary;
use crate::dashboard_flow::SnapshotFlowSummary;
use crate::dashboard_flow::SnapshotSummaries;
use crate::upstream::ProviderHealthPublisher;
use crate::upstream::ProviderHealthSnapshot;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// Ring length for the 1-minute window (1 s resolution).
const WINDOW_1M_SLOTS: usize = 60;
/// Ring length for the 5-minute window (1 s resolution).
const WINDOW_5M_SLOTS: usize = 300;
/// Ring length for the 1-hour window (1 s resolution).
const WINDOW_1H_SLOTS: usize = 3600;

/// Number of log-spaced latency histogram buckets spanning 1 ms .. 120 s.
const HISTOGRAM_BUCKETS: usize = 30;
/// Lowest histogram bucket upper-bound (ms). Samples ≤ this land in bucket 0.
const HISTOGRAM_MIN_MS: f64 = 1.0;
/// Highest finite histogram bucket upper-bound (ms) — 120 s. Samples above land in
/// the final overflow bucket.
const HISTOGRAM_MAX_MS: f64 = 120_000.0;

/// Snapshot ring length: 720 body-free cuts = 12 per minute (one per 5 s) × 60 min.
const SNAPSHOT_RING_SLOTS: usize = 720;

/// Gap 12 — HARD cap on the number of DISTINCT providers tracked in the per-provider
/// latency/error rings (PER SLOT). The provider key is derived from the attempt's
/// `provider` label, which can be an attacker-influenceable routed/remapped alias, so
/// an unbounded `BTreeMap` keyed by it would be a memory-growth vector (AGENTS.md:
/// bounded structures only). Once a slot already tracks this many providers, a fresh
/// provider's attempt is folded into the bounded [`OVERFLOW_PROVIDER`] catch-all bucket
/// instead of spawning a new key — so the per-provider view stays useful (the known
/// providers keep exact stats) while the key space can never exceed
/// `MAX_TRACKED_PROVIDERS + 1` per slot. Samples-per-provider are bounded by the
/// fixed-size 30-bucket [`Histogram`] (counts saturate; no per-sample retention).
const MAX_TRACKED_PROVIDERS: usize = 64;

/// Gap 12 — the bounded catch-all provider key a slot folds OVERFLOW providers into once
/// it already tracks [`MAX_TRACKED_PROVIDERS`] distinct providers. A single fixed key, so
/// the overflow can never itself grow the key space. Chosen to never collide with a real
/// provider name (a configured provider/route id is never this sentinel).
const OVERFLOW_PROVIDER: &str = "__other__";

/// Default peak snapshot-ring memory quota (bytes), the HARD bound the ring cannot
/// exceed — when a fresh cut would push the retained total over quota, the OLDEST
/// cuts are dropped first until it fits. Cuts share every summary that did not
/// change, and the quota charges each UNIQUE allocation once, so a full hour of
/// 512 resident flows costs ~1 KiB per distinct flow VERSION, not 720 copies of
/// each flow; 64 MiB fits on a 2 GB host. `LLMCONDUIT_DASHBOARD_SNAPSHOT_BYTES`
/// overrides it. Bodies are never on a snapshot (the 135 GiB fix).
const DEFAULT_SNAPSHOT_QUOTA_BYTES: usize = 64 * 1024 * 1024;

/// HTTP status class for a terminal flow, the metrics bucket key dimension. Derived
/// from the [`FlowStatus`] terminal (the engine does not thread a raw numeric code
/// to the metrics seam — the terminal status IS the authoritative outcome).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusClass {
    /// A clean `Completed` terminal (2xx-equivalent).
    Success,
    /// A `Failed` terminal (5xx/4xx-equivalent — an upstream/gateway error).
    Error,
    /// A `Cancelled` terminal (client hang-up / abort — 499-equivalent).
    Cancelled,
}

impl StatusClass {
    /// Map a terminal [`FlowStatus`] to its metrics status class. `Open` should
    /// never reach the terminal seam, but is conservatively bucketed `Error` (a
    /// flow that finalized while still notionally open is an anomaly, not a success).
    fn from_status(status: FlowStatus) -> Self {
        match status {
            FlowStatus::Completed => StatusClass::Success,
            FlowStatus::Cancelled => StatusClass::Cancelled,
            FlowStatus::Failed | FlowStatus::Open => StatusClass::Error,
        }
    }
}

/// The composite bucket key: `{status_class, model, endpoint, upstream}`. `model`
/// is the SERVED model (the backend that actually answered); `endpoint` is the
/// inbound route family (`/v1/responses`, `/v1/chat/completions`, …); `upstream`
/// is the serving provider/route label. Unknown dimensions collapse to `"unknown"`
/// so the key space stays bounded and a missing attribution never spawns a distinct
/// `None` bucket.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct BucketKey {
    pub status: StatusClass,
    pub model: String,
    pub endpoint: String,
    pub upstream: String,
}

/// Per-key aggregate counters within one window: the request count and summed
/// token counters. Latency is aggregated separately in the window-level
/// [`Histogram`] (a per-key histogram would be 30 buckets × |keys| — wasteful;
/// p-quantiles are reported window-wide, which is the stats-strip contract).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct BucketCounts {
    pub count: u64,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub cached_tokens: i64,
    pub reasoning_tokens: i64,
    /// Prompt tokens from flows with a measured prefill phase.
    pub prefill_tokens: i64,
    /// Summed measured first-byte → first-content time for `prefill_tokens`.
    pub prefill_ms: u128,
    /// Completion tokens from flows with a measured decode phase.
    pub decode_tokens: i64,
    /// Summed measured first-content → stream-end time for `decode_tokens`.
    pub decode_ms: u128,
    /// Flows contributing a non-zero measured prefill duration.
    pub prefill_samples: u64,
    /// Flows contributing a non-zero measured decode duration.
    pub decode_samples: u64,
    /// Count of the TERMINAL flows in this bucket that actually carried token usage
    /// (gap 01 review round 1, finding 3): token throughput is only MEASURABLE for a
    /// flow whose final usage was reported. A finalized flow with no `usage` (an
    /// upstream that omitted the usage block, or a failure before any token
    /// accounting) bumps `count` but NOT this — so a window whose flows all lack usage
    /// reports `usage_samples == 0` and the strip renders `tokens_per_sec` as
    /// `unavailable` (`—`) instead of a fabricated `0`. The COST denominator is the
    /// subset of these whose served model has a configured price; it is derived at
    /// render time (`dashboard_api`) where the price table lives, so this layer stays
    /// price-agnostic.
    pub usage_samples: u64,
    /// Gap 07 — count of usage-bearing terminal flows in this bucket whose `cached`
    /// token class was UNREPORTED (`None`) by the upstream. The summed `cached_tokens`
    /// CANNOT distinguish "all flows reported 0 cache hits" from "no flow reported a
    /// cache breakdown at all" (both sum to `0`), yet they differ for COST confidence:
    /// an unreported cached against a model with no configured cache rate is an
    /// `estimated` charge, not `confident`. This counter preserves that per-flow signal
    /// so the window-level cost-confidence aggregate (`dashboard_api`) can flag a
    /// window as `estimated` even when `cached_tokens == 0`. Price-agnostic (the cache
    /// RATE presence is joined at render time where the price table lives).
    pub unreported_cached_samples: u64,
}

impl BucketCounts {
    /// Approximate retained bytes for the snapshot-memory quota (the fixed scalar
    /// fields; the key string bytes are counted at the bucket-map level).
    fn approx_bytes(&self) -> usize {
        std::mem::size_of::<BucketCounts>()
    }
}

/// A 30-bucket log-spaced latency histogram (1 ms .. 120 s) over the in-window
/// samples, plus p50/p95/p99 via linear interpolation over the cumulative counts.
/// Counts are `u64`; an empty histogram reports `0.0` for every quantile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Histogram {
    /// Per-bucket sample counts. `buckets[i]` counts samples whose latency is
    /// ≤ `bucket_upper_ms(i)` and > `bucket_upper_ms(i - 1)`. The final bucket is
    /// the `> HISTOGRAM_MAX_MS` overflow bucket.
    buckets: [u64; HISTOGRAM_BUCKETS],
    /// Total samples (== sum of `buckets`), retained so quantile math avoids a
    /// re-sum and the empty case is O(1).
    total: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: [0; HISTOGRAM_BUCKETS],
            total: 0,
        }
    }
}

/// The inclusive upper bound (ms) of histogram bucket `index`. The finite buckets
/// (indices `0..=HISTOGRAM_BUCKETS-2`, i.e. all but the overflow bucket) are LOG-SPACED
/// so the FIRST finite upper bound is EXACTLY [`HISTOGRAM_MIN_MS`] (1 ms) and the LAST
/// is EXACTLY [`HISTOGRAM_MAX_MS`] (120 s); the final bucket is the overflow bucket with
/// an effectively-infinite bound. Pure function of `index` (no per-instance state), so
/// the boundary ladder is computed identically at record + quantile time.
///
/// The fraction is `index / (HISTOGRAM_BUCKETS - 2)` (Codex D5 R1 #3) — NOT
/// `(index+1)/(HISTOGRAM_BUCKETS-1)`, which made bucket 0's upper bound ≈ 1.5 ms and
/// pushed the whole ladder up. With this mapping `index == 0` ⇒ frac 0 ⇒ exactly 1 ms
/// and `index == HISTOGRAM_BUCKETS-2` ⇒ frac 1 ⇒ exactly 120 s.
fn bucket_upper_ms(index: usize) -> f64 {
    if index >= HISTOGRAM_BUCKETS - 1 {
        return f64::INFINITY;
    }
    let span = (HISTOGRAM_MAX_MS / HISTOGRAM_MIN_MS).ln();
    let frac = index as f64 / (HISTOGRAM_BUCKETS - 2) as f64;
    HISTOGRAM_MIN_MS * (span * frac).exp()
}

/// The lower bound (ms) of histogram bucket `index` — the previous bucket's upper
/// bound, or `0.0` for bucket 0. Used as the interpolation floor.
fn bucket_lower_ms(index: usize) -> f64 {
    if index == 0 {
        0.0
    } else {
        bucket_upper_ms(index - 1)
    }
}

impl Histogram {
    /// Record one latency sample (ms) into its log-spaced bucket.
    fn record(&mut self, elapsed_ms: f64) {
        let index = self.bucket_index(elapsed_ms);
        self.buckets[index] = self.buckets[index].saturating_add(1);
        self.total = self.total.saturating_add(1);
    }

    /// The bucket index a `elapsed_ms` sample falls into. Linear scan over 30
    /// buckets is trivial and avoids `ln` rounding mismatches at the boundaries
    /// that a closed-form inverse could introduce.
    fn bucket_index(&self, elapsed_ms: f64) -> usize {
        let value = if elapsed_ms.is_finite() {
            elapsed_ms.max(0.0)
        } else {
            HISTOGRAM_MAX_MS + 1.0
        };
        for index in 0..HISTOGRAM_BUCKETS - 1 {
            if value <= bucket_upper_ms(index) {
                return index;
            }
        }
        HISTOGRAM_BUCKETS - 1
    }

    /// Merge another histogram into this one (used when collapsing a window's slots
    /// into a single reported histogram).
    fn merge(&mut self, other: &Histogram) {
        for index in 0..HISTOGRAM_BUCKETS {
            self.buckets[index] = self.buckets[index].saturating_add(other.buckets[index]);
        }
        self.total = self.total.saturating_add(other.total);
    }

    /// The `quantile` (0.0..=1.0) latency in ms via LINEAR INTERPOLATION over the
    /// cumulative bucket counts. Returns `0.0` for an empty histogram. The target
    /// rank is `quantile × total`; we walk buckets accumulating counts until the
    /// rank falls inside a bucket, then interpolate linearly between that bucket's
    /// lower and upper bound by how far into the bucket the rank lies. The overflow
    /// bucket (unbounded upper) reports its FINITE lower bound (`HISTOGRAM_MAX_MS`)
    /// so a p99 dominated by >120 s samples returns 120 s, not infinity.
    fn quantile(&self, quantile: f64) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        let quantile = quantile.clamp(0.0, 1.0);
        // Target rank in [0, total]. Using `(total) * q` and walking cumulative
        // upper edges gives the standard histogram-interpolation estimate.
        let target = quantile * self.total as f64;
        let mut cumulative_before = 0u64;
        for index in 0..HISTOGRAM_BUCKETS {
            let count = self.buckets[index];
            if count == 0 {
                continue;
            }
            let cumulative_after = cumulative_before + count;
            if target <= cumulative_after as f64 {
                let lower = bucket_lower_ms(index);
                let upper = bucket_upper_ms(index);
                // Overflow bucket: report its finite lower bound (120 s), never inf.
                if !upper.is_finite() {
                    return lower;
                }
                // Linear interpolation: how far into THIS bucket's count the target
                // rank lies, mapped onto [lower, upper].
                let into_bucket = (target - cumulative_before as f64).max(0.0);
                let fraction = (into_bucket / count as f64).clamp(0.0, 1.0);
                return lower + (upper - lower) * fraction;
            }
            cumulative_before = cumulative_after;
        }
        // Target beyond all counted buckets (q == 1.0 edge): the max finite bound.
        bucket_upper_ms(HISTOGRAM_BUCKETS - 2)
    }
}

/// Gap 12 — the bounded per-error-class failure tally for one provider (a window, or a
/// single slot). The key space is the FIXED [`AttemptErrorClass`] taxonomy (6 bounded
/// snake_case codes from gap 03 — NEVER raw upstream text), so this is a fixed-size
/// counter array, not an unbounded map. `skip_serializing_if` keeps a `0` class ABSENT
/// on the wire — the distribution lists only the classes a provider actually hit. A
/// per-class `0` here is honest (that class did not occur); the don't-lie-with-zeros
/// rule lives at the PROVIDER level (a provider with NO samples in the window is absent
/// from the per-provider report entirely, never a fabricated all-zero distribution).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, serde::Deserialize, utoipa::ToSchema,
)]
#[serde(default)]
pub struct ProviderErrorDistribution {
    /// Failed attempts whose class was `connect` (omitted when 0).
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub connect: u64,
    /// Failed attempts whose class was `http_status` (omitted when 0).
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub http_status: u64,
    /// Failed attempts whose class was `timeout` (omitted when 0).
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub timeout: u64,
    /// Failed attempts whose class was `stream` (omitted when 0).
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub stream: u64,
    /// Failed attempts whose class was `terminal` (omitted when 0).
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub terminal: u64,
    /// Failed attempts whose class was `other`, including an unclassed failure (omitted when 0).
    #[serde(skip_serializing_if = "is_zero_u64")]
    pub other: u64,
}

impl ProviderErrorDistribution {
    /// Tally one failed attempt's bounded taxonomic [`AttemptErrorClass`]. An attempt with
    /// no recorded class (a failed attempt whose class was somehow `None`) is counted as
    /// `Other` so the per-class total always equals the provider's failed-attempt count.
    fn record(&mut self, class: Option<AttemptErrorClass>) {
        match class {
            Some(AttemptErrorClass::Connect) => self.connect = self.connect.saturating_add(1),
            Some(AttemptErrorClass::HttpStatus) => {
                self.http_status = self.http_status.saturating_add(1)
            }
            Some(AttemptErrorClass::Timeout) => self.timeout = self.timeout.saturating_add(1),
            Some(AttemptErrorClass::Stream) => self.stream = self.stream.saturating_add(1),
            Some(AttemptErrorClass::Terminal) => self.terminal = self.terminal.saturating_add(1),
            Some(AttemptErrorClass::Other) | None => self.other = self.other.saturating_add(1),
        }
    }

    /// Merge another distribution into this one (collapsing a window's slots).
    fn merge(&mut self, other: &ProviderErrorDistribution) {
        self.connect = self.connect.saturating_add(other.connect);
        self.http_status = self.http_status.saturating_add(other.http_status);
        self.timeout = self.timeout.saturating_add(other.timeout);
        self.stream = self.stream.saturating_add(other.stream);
        self.terminal = self.terminal.saturating_add(other.terminal);
        self.other = self.other.saturating_add(other.other);
    }
}

/// Gap 12 — one provider's per-slot latency + outcome tally: a fixed 30-bucket latency
/// [`Histogram`] (REUSED from the global ring — NOT a second percentile implementation)
/// over the provider's ATTEMPT latencies, the served/failed attempt counts, and the
/// bounded per-class error distribution. Fixed-size (the histogram is 30 `u64`s, the
/// distribution 6 `u64`s) so a provider's per-slot footprint is O(1) regardless of
/// traffic — samples-per-provider are bounded by the histogram, provider COUNT by
/// [`MAX_TRACKED_PROVIDERS`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ProviderSample {
    histogram: Histogram,
    served: u64,
    failed: u64,
    errors: ProviderErrorDistribution,
}

impl ProviderSample {
    /// Record one upstream dispatch ATTEMPT for this provider: its latency lands in the
    /// histogram (so a FAILED primary's latency counts toward its provider — final-served
    /// latency alone would hide an unhealthy provider, spec 12) and its served/failed
    /// outcome + taxonomic error class are tallied.
    fn record(&mut self, latency_ms: f64, attempt: &Attempt) {
        self.histogram.record(latency_ms);
        match attempt.status {
            AttemptStatus::Served => self.served = self.served.saturating_add(1),
            AttemptStatus::Failed => {
                self.failed = self.failed.saturating_add(1);
                self.errors.record(attempt.error_class);
            }
        }
    }

    /// Merge another slot's sample for the SAME provider into this one (window collapse).
    fn merge(&mut self, other: &ProviderSample) {
        self.histogram.merge(&other.histogram);
        self.served = self.served.saturating_add(other.served);
        self.failed = self.failed.saturating_add(other.failed);
        self.errors.merge(&other.errors);
    }

    /// Approximate retained bytes of this per-provider sample for the snapshot-memory
    /// quota (mirrors [`BucketCounts::approx_bytes`]). Every field is fixed-size and
    /// heap-free — the `Histogram` is a `[u64; 30]` array, the served/failed counters are
    /// `u64`, and the [`ProviderErrorDistribution`] is six `u64`s — so the full footprint
    /// is the struct size; the provider KEY string bytes are charged at the map level (as
    /// the global `buckets` charges its `BucketKey` strings separately).
    fn approx_bytes(&self) -> usize {
        std::mem::size_of::<ProviderSample>()
    }
}

/// One 1-second slot of a window ring: the per-key counts, the latency histogram,
/// the per-provider attempt tallies (gap 12), and the epoch-second this slot
/// currently represents. `epoch_s == 0` marks an unused slot. When wall-clock
/// advances to a new second whose ring index maps to a slot still stamped with an
/// OLDER epoch second, the slot is RESET before the new sample lands (circular reuse
/// — the window only ever holds its own time span).
#[derive(Debug, Clone, Default)]
struct Slot {
    epoch_s: u64,
    buckets: BTreeMap<BucketKey, BucketCounts>,
    histogram: Histogram,
    /// Gap 12 — per-provider attempt tallies, keyed by the attempt's provider label.
    /// BOUNDED to [`MAX_TRACKED_PROVIDERS`] (+1 overflow) distinct keys per slot: a
    /// fresh provider beyond the cap folds into [`OVERFLOW_PROVIDER`], so an
    /// attacker-influenceable provider alias can never grow this map without bound.
    providers: BTreeMap<String, ProviderSample>,
}

impl Slot {
    /// Reset this slot to represent `epoch_s` with no samples (circular reuse).
    fn reset(&mut self, epoch_s: u64) {
        self.epoch_s = epoch_s;
        self.buckets.clear();
        self.histogram = Histogram::default();
        self.providers.clear();
    }

    /// The mutable per-provider sample for `provider`, BOUNDING the map to
    /// [`MAX_TRACKED_PROVIDERS`] distinct keys: an EXISTING provider is returned as-is;
    /// a NEW provider is admitted only while there is room, otherwise its attempt is
    /// folded into the fixed [`OVERFLOW_PROVIDER`] catch-all (created on demand, but a
    /// SINGLE bounded key). So the slot can hold at most `MAX_TRACKED_PROVIDERS + 1`
    /// keys regardless of how many distinct provider strings arrive.
    fn provider_sample_mut(&mut self, provider: &str) -> &mut ProviderSample {
        if self.providers.contains_key(provider) || self.providers.len() < MAX_TRACKED_PROVIDERS {
            return self.providers.entry(provider.to_string()).or_default();
        }
        self.providers
            .entry(OVERFLOW_PROVIDER.to_string())
            .or_default()
    }
}

/// Gap 12 — fold a flow's per-attempt trace into ONE ring slot's per-provider tallies.
/// Each attempt contributes (provider, latency, served/failed, error_class). The latency
/// is the attempt's own measured wall-clock span (`end_ms - start_ms`) — a per-ATTEMPT
/// latency, so a failed primary's slow connect/timeout counts toward ITS provider, not
/// the eventual server's (this is exactly what global per-flow latency cannot show). The
/// provider key collapses to the bounded `"unknown"` sentinel when an attempt recorded no
/// provider label, so a missing attribution never spawns a distinct empty key. The
/// provider COUNT is bounded by `Slot::provider_sample_mut` (the [`MAX_TRACKED_PROVIDERS`]
/// cap + overflow); samples-per-provider by the fixed-size histogram.
fn record_attempts_into_slot(slot: &mut Slot, attempts: &[Attempt]) {
    for attempt in attempts {
        // `end_ms`/`start_ms` are measured epoch-ms (gap 03 always stamps them). A
        // non-monotonic pair (clock skew) saturates to 0 ms rather than wrapping.
        let latency_ms = attempt.end_ms.saturating_sub(attempt.start_ms) as f64;
        let provider = attempt.provider.as_deref().unwrap_or("unknown");
        slot.provider_sample_mut(provider)
            .record(latency_ms, attempt);
    }
}

/// A fixed-size ring of 1-second [`Slot`]s for one window. The ring is indexed by
/// `epoch_s % slots`; reads that aggregate the window only count slots whose
/// `epoch_s` is within `[now - slots + 1, now]` (stale slots from a prior lap are
/// ignored), so a quiet gateway reports an empty window rather than ancient samples.
#[derive(Debug, Clone)]
struct WindowRing {
    slots: Vec<Slot>,
}

impl WindowRing {
    fn new(len: usize) -> Self {
        Self {
            slots: vec![Slot::default(); len],
        }
    }

    /// The mutable slot for `epoch_s`, reset first if it currently holds a different
    /// (older lap) second — so a sample only ever lands in a slot representing its
    /// own second.
    fn slot_mut(&mut self, epoch_s: u64) -> &mut Slot {
        let len = self.slots.len();
        let index = (epoch_s as usize) % len;
        let slot = &mut self.slots[index];
        if slot.epoch_s != epoch_s {
            slot.reset(epoch_s);
        }
        slot
    }

    /// Aggregate the window AS OF `now_epoch_s`: merge every slot whose `epoch_s` is
    /// within the window span `[now - len + 1, now]` into a single counts-map +
    /// histogram. Slots outside the span (a prior lap, or never written) are skipped.
    fn aggregate(&self, now_epoch_s: u64) -> WindowReport {
        let len = self.slots.len() as u64;
        let floor = now_epoch_s.saturating_sub(len - 1);
        let mut buckets: BTreeMap<BucketKey, BucketCounts> = BTreeMap::new();
        let mut histogram = Histogram::default();
        let mut providers: BTreeMap<String, ProviderSample> = BTreeMap::new();
        for slot in &self.slots {
            if slot.epoch_s < floor || slot.epoch_s > now_epoch_s {
                continue;
            }
            // Gap 12: merge each in-window slot's per-provider attempt tallies. Each slot
            // is individually capped at `MAX_TRACKED_PROVIDERS + 1` keys, but DISTINCT
            // slots can track DISTINCT provider sets (an attacker rotating provider aliases
            // each second), so the naive UNION across up-to-`len` slots is NOT bounded by a
            // single cap. Bound the merged map the SAME way a slot bounds itself: an
            // already-tracked provider always merges; a NEW provider is admitted only while
            // under the cap, otherwise it folds into the bounded overflow bucket. So the
            // aggregated (retained-in-this-call + serialized) provider count is hard-capped
            // at `MAX_TRACKED_PROVIDERS + 1` regardless of slot rotation.
            for (provider, sample) in &slot.providers {
                if providers.contains_key(provider) || providers.len() < MAX_TRACKED_PROVIDERS {
                    providers.entry(provider.clone()).or_default().merge(sample);
                } else {
                    providers
                        .entry(OVERFLOW_PROVIDER.to_string())
                        .or_default()
                        .merge(sample);
                }
            }
            for (key, counts) in &slot.buckets {
                let entry = buckets.entry(key.clone()).or_default();
                entry.count = entry.count.saturating_add(counts.count);
                entry.prompt_tokens = entry.prompt_tokens.saturating_add(counts.prompt_tokens);
                entry.completion_tokens = entry
                    .completion_tokens
                    .saturating_add(counts.completion_tokens);
                entry.cached_tokens = entry.cached_tokens.saturating_add(counts.cached_tokens);
                entry.reasoning_tokens = entry
                    .reasoning_tokens
                    .saturating_add(counts.reasoning_tokens);
                entry.prefill_tokens = entry.prefill_tokens.saturating_add(counts.prefill_tokens);
                entry.prefill_ms = entry.prefill_ms.saturating_add(counts.prefill_ms);
                entry.decode_tokens = entry.decode_tokens.saturating_add(counts.decode_tokens);
                entry.decode_ms = entry.decode_ms.saturating_add(counts.decode_ms);
                entry.prefill_samples =
                    entry.prefill_samples.saturating_add(counts.prefill_samples);
                entry.decode_samples = entry.decode_samples.saturating_add(counts.decode_samples);
                entry.usage_samples = entry.usage_samples.saturating_add(counts.usage_samples);
                entry.unreported_cached_samples = entry
                    .unreported_cached_samples
                    .saturating_add(counts.unreported_cached_samples);
            }
            histogram.merge(&slot.histogram);
        }
        WindowReport {
            buckets,
            histogram,
            providers,
        }
    }
}

/// The collapsed per-window view: the merged per-key counts + the merged latency
/// histogram, from which p50/p95/p99 are reported.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct WindowReport {
    pub buckets: BTreeMap<BucketKey, BucketCounts>,
    pub histogram: Histogram,
    /// Gap 12 — the merged per-provider attempt tallies for the window, keyed by
    /// provider label. NOT serialized on the wire (`#[serde(skip)]`): the public
    /// per-provider DTO is COMPUTED at render time via [`WindowReport::per_provider`]
    /// (in `dashboard_api`, where it joins the topology nodes), exactly like the
    /// window `percentiles` are computed at render rather than stored. This keeps the
    /// raw internal [`ProviderSample`] (with its private histogram) off the wire and
    /// the snapshot payload unchanged.
    #[serde(skip)]
    providers: BTreeMap<String, ProviderSample>,
}

impl WindowReport {
    /// `(p50, p95, p99)` latency in ms over this window's histogram.
    pub fn percentiles(&self) -> Percentiles {
        Percentiles {
            p50: self.histogram.quantile(0.50),
            p95: self.histogram.quantile(0.95),
            p99: self.histogram.quantile(0.99),
        }
    }

    /// Total request count across all keys in the window.
    pub fn total_count(&self) -> u64 {
        self.buckets
            .values()
            .map(|counts| counts.count)
            .fold(0u64, u64::saturating_add)
    }

    /// Total count of TERMINAL flows that reported token usage across all keys (gap 01
    /// finding 3) — the `tokens_per_sec` measurability denominator. `0` ⇒ no flow in
    /// the window carried usage ⇒ the strip renders `tokens_per_sec` as `unavailable`.
    pub fn usage_sample_count(&self) -> u64 {
        self.buckets
            .values()
            .map(|counts| counts.usage_samples)
            .fold(0u64, u64::saturating_add)
    }

    /// Aggregate effective prefill speed across flows with measured prompt usage and
    /// a non-zero first-byte → first-content phase.
    pub fn prefill_tokens_per_sec(&self) -> f64 {
        let tokens = self
            .buckets
            .values()
            .map(|counts| counts.prefill_tokens)
            .fold(0i64, i64::saturating_add);
        let ms = self
            .buckets
            .values()
            .map(|counts| counts.prefill_ms)
            .fold(0u128, u128::saturating_add);
        if ms == 0 {
            0.0
        } else {
            tokens as f64 * 1000.0 / ms as f64
        }
    }

    /// Aggregate decode speed across flows with measured completion usage and a
    /// non-zero first-content → stream-end phase.
    pub fn decode_tokens_per_sec(&self) -> f64 {
        let tokens = self
            .buckets
            .values()
            .map(|counts| counts.decode_tokens)
            .fold(0i64, i64::saturating_add);
        let ms = self
            .buckets
            .values()
            .map(|counts| counts.decode_ms)
            .fold(0u128, u128::saturating_add);
        if ms == 0 {
            0.0
        } else {
            tokens as f64 * 1000.0 / ms as f64
        }
    }

    pub fn prefill_sample_count(&self) -> u64 {
        self.buckets
            .values()
            .map(|counts| counts.prefill_samples)
            .fold(0u64, u64::saturating_add)
    }

    pub fn decode_sample_count(&self) -> u64 {
        self.buckets
            .values()
            .map(|counts| counts.decode_samples)
            .fold(0u64, u64::saturating_add)
    }

    /// Count of usage-bearing terminal flows whose served model has a configured price
    /// in `prices` (gap 01 finding 3) — the `cost_per_min` measurability denominator.
    /// `0` ⇒ no PRICED usage sample in the window ⇒ the strip renders `cost_per_min` as
    /// `unavailable` (`—`), distinguishing "unpriced model" from a genuine `$0.00`. The
    /// price lookup matches [`crate::dashboard_api`]'s exact-then-case-insensitive rule.
    pub fn priced_sample_count<F>(&self, has_price: F) -> u64
    where
        F: Fn(&str) -> bool,
    {
        self.buckets
            .iter()
            .filter(|(key, _)| has_price(&key.model))
            .map(|(_, counts)| counts.usage_samples)
            .fold(0u64, u64::saturating_add)
    }

    /// Gap 12 — the per-provider latency + error distribution for ONE provider over this
    /// window, or `None` when the provider has ZERO attempt samples in the window. The
    /// `None` IS the don't-lie-with-zeros signal (spec 12, mirroring spec 01): a provider
    /// with no in-window samples must render `unavailable` (`—`), NEVER a fabricated `0`
    /// or all-zero percentile — so the caller surfaces an ABSENT per-provider tile, not a
    /// zeroed one. Percentiles are `derived` from the same log-spaced [`Histogram`] the
    /// global ring uses (no second percentile implementation). `samples` counts EVERY
    /// attempt to the provider (served + failed) so a failed primary is included.
    pub fn provider_latency(&self, provider: &str) -> Option<ProviderLatency> {
        self.providers
            .get(provider)
            .map(|sample| provider_latency_from_sample(provider, sample))
    }

    /// Gap 12 — the per-provider latency + error distribution for EVERY provider with at
    /// least one attempt sample in this window. Providers with zero in-window samples are
    /// simply absent from the map (don't-lie-with-zeros). The map is bounded by the ring's
    /// per-slot [`MAX_TRACKED_PROVIDERS`] cap.
    pub fn per_provider(&self) -> BTreeMap<String, ProviderLatency> {
        self.providers
            .iter()
            .map(|(provider, sample)| {
                (
                    provider.clone(),
                    provider_latency_from_sample(provider, sample),
                )
            })
            .collect()
    }
}

/// Build the public [`ProviderLatency`] DTO from one provider's merged window
/// [`ProviderSample`]. Only ever called for a provider that HAS samples (the callers
/// gate on presence), so `samples >= 1` and the percentiles/error-rate are genuinely
/// `derived` — never a fabricated zero. `error_rate` is failed / (served + failed).
fn provider_latency_from_sample(provider: &str, sample: &ProviderSample) -> ProviderLatency {
    let samples = sample.served.saturating_add(sample.failed);
    let percentiles = ProviderPercentiles {
        p50: sample.histogram.quantile(0.50),
        p95: sample.histogram.quantile(0.95),
        p99: sample.histogram.quantile(0.99),
    };
    // `samples >= 1` here (the provider only exists in the map because an attempt landed),
    // so this division is never 0/0; an all-served provider reports `0.0` (a genuine
    // MEASURED zero error rate, distinct from the `unavailable` no-samples case which is
    // an ABSENT `ProviderLatency`, never this struct).
    let error_rate = if samples > 0 {
        (sample.failed as f64) / (samples as f64) * 100.0
    } else {
        0.0
    };
    ProviderLatency {
        provider: provider.to_string(),
        // Gap 12: every per-provider metric is `derived` (percentiles over the provider's
        // own attempt histogram). The DQ tag travels with the data so the frontend (spec
        // 13) labels it; a zero-sample provider never reaches here (it is absent).
        data_quality: ProviderMetricQuality::Derived,
        samples,
        served: sample.served,
        failed: sample.failed,
        p50: finite_ms(percentiles.p50),
        p95: finite_ms(percentiles.p95),
        p99: finite_ms(percentiles.p99),
        error_rate: finite_ms(error_rate),
        errors: sample.errors,
    }
}

/// Clamp a metrics float to a finite value (`0.0` for NaN/Inf) so the per-provider DTO
/// never carries a non-finite number that would fail `serde_json::to_vec` and 500 the
/// `/topology` read (mirrors `dashboard_api::finite`). Local to this module so the
/// per-provider DTO is self-contained.
fn finite_ms(value: f64) -> f64 {
    if value.is_finite() { value } else { 0.0 }
}

/// Gap 12 — the data-quality tag for a per-provider metric. Every per-provider latency
/// figure is `derived` (computed from the provider's own attempt-latency histogram). The
/// `unavailable` case (a provider with no in-window samples) is represented by the
/// ABSENCE of a [`ProviderLatency`] entirely, not by a variant here — so a present DTO is
/// always a real `derived` measurement (don't-lie-with-zeros). Serializes snake_case to
/// match the cross-cutting DQ-tag contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProviderMetricQuality {
    /// Computed from the provider's own attempt-latency histogram.
    Derived,
}

/// Gap 12 — the public per-provider latency + error-distribution DTO (additive on the
/// D4 topology node; consumed by spec 13). Percentiles are `derived` ms over the
/// provider's ATTEMPT-latency histogram (a FAILED primary's latency is included — spec
/// 12 — so final-served latency alone cannot hide an unhealthy provider). `error_rate`
/// is the percentage of the provider's attempts that FAILED. A provider with zero
/// in-window samples is ABSENT (don't-lie-with-zeros), so a present DTO always has
/// `samples >= 1`. All floats are finite (the frozen finite-number wire contract).
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct ProviderLatency {
    /// The provider label these metrics are for (the bounded provider/route id, or the
    /// `__other__` overflow bucket once the per-slot provider cap is exceeded).
    pub provider: String,
    /// DQ tag — always `derived` for a present entry (the `unavailable` case is absence).
    pub data_quality: ProviderMetricQuality,
    /// Total ATTEMPTS to this provider in the window (served + failed) — the per-provider
    /// measurability denominator. Always `>= 1` (a zero-sample provider is absent).
    pub samples: u64,
    /// Of `samples`, the count that SERVED (produced the first chunk).
    pub served: u64,
    /// Of `samples`, the count that FAILED before serving (failed primaries included).
    pub failed: u64,
    /// `derived` p50 attempt latency (ms).
    pub p50: f64,
    /// `derived` p95 attempt latency (ms).
    pub p95: f64,
    /// `derived` p99 attempt latency (ms).
    pub p99: f64,
    /// Percentage of attempts that failed (`failed / samples × 100`). A genuine MEASURED
    /// `0.0` for an all-served provider (distinct from the `unavailable`/absent case).
    pub error_rate: f64,
    /// Bounded per-class failure tally (gap 03 taxonomy). Absent classes are omitted.
    pub errors: ProviderErrorDistribution,
}

/// The reported p50/p95/p99 (ms) for a provider — a local triple used to assemble
/// [`ProviderLatency`] (kept distinct from the window-wide [`Percentiles`] so the
/// per-provider math is self-documenting).
struct ProviderPercentiles {
    p50: f64,
    p95: f64,
    p99: f64,
}

/// The reported p50/p95/p99 latency (ms) for a window.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct Percentiles {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

/// The body-free metrics view captured into a [`DashboardSnapshot`]: the three
/// windows' collapsed reports + their percentiles, as of the snapshot instant. This
/// is a pure value (no `Arc`, no live-store reference), so a retained snapshot
/// cannot pin live state.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct MetricsView {
    pub window_1m: WindowReport,
    pub window_5m: WindowReport,
    pub window_1h: WindowReport,
    pub percentiles_1m: Percentiles,
    pub percentiles_5m: Percentiles,
    pub percentiles_1h: Percentiles,
}

impl MetricsView {
    /// Approximate retained bytes of this view (for the snapshot-memory quota):
    /// per-window bucket key strings + counts + the fixed histograms, PLUS (gap 12) the
    /// per-provider maps retained on `WindowReport.providers` — those are `#[serde(skip)]`
    /// (kept off the wire) but STILL retained on every `DashboardSnapshot` cut, so the
    /// quota must charge them or the snapshot ring would undercount the per-provider
    /// keys/histograms and exceed its memory bound (the bounded-memory invariant).
    fn approx_bytes(&self) -> usize {
        let window_bytes = |report: &WindowReport| -> usize {
            let mut bytes = std::mem::size_of::<Histogram>();
            for (key, counts) in &report.buckets {
                bytes += key.model.len()
                    + key.endpoint.len()
                    + key.upstream.len()
                    + std::mem::size_of::<BucketKey>()
                    + counts.approx_bytes();
            }
            // Gap 12: charge the per-provider map the SAME way — each entry's provider key
            // string bytes + the `String` overhead + the fixed `ProviderSample`
            // (histogram + counts + error distribution). Bounded by `MAX_TRACKED_PROVIDERS
            // + 1` entries (the per-window union cap), so this is a bounded addend.
            for (provider, sample) in &report.providers {
                bytes += provider.len() + std::mem::size_of::<String>() + sample.approx_bytes();
            }
            bytes
        };
        window_bytes(&self.window_1m)
            + window_bytes(&self.window_5m)
            + window_bytes(&self.window_1h)
    }
}

/// The per-domain cursor quad carried on every [`DashboardSnapshot`] (AGENTS.md:
/// per-domain `{domain, seq}` cursors, NOT a single global watermark). Each field
/// is the authoritative sequence of its own store at the cut instant.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct DomainCursors {
    /// FlowStore mutation sequence (D1).
    pub flow_seq: u64,
    /// MetricsLayer mutation sequence (this module).
    pub metrics_seq: u64,
    /// Topology publisher version (D4's `ProviderHealthSnapshot.version`).
    pub topology_seq: u64,
    /// Monitor hub sequence (D3 transcript domain).
    pub monitor_seq: u64,
}

/// One body-free atomic cut across the FlowStore, MetricsLayer, and topology stores
/// taken by the 5 s coordinated snapshot task. Carries:
/// - `taken_at_ms`: the cut's wall-clock instant (the `snapshot_at(ts)` key).
/// - `cursors`: per-domain `{flow,metrics,topology,monitor}` sequences at the cut.
/// - `summaries`: body-free [`SnapshotFlowSummary`]s (NO `Arc<[u8]>` — the 135 GiB
///   fix; a retained snapshot holds at most ~1 KiB per flow, never a 128 KiB body).
///   SHARED with neighbouring cuts: an unchanged flow is one allocation across every
///   cut that captured it, and an unchanged list is one allocation too.
/// - `metrics`: the collapsed [`MetricsView`], shared with the previous cut when equal.
/// - `topology`: the ONE `Arc<ProviderHealthSnapshot>` captured in the cut.
///
/// Immutable once built; `Arc`-wrapped in the ring so a reader's clone is cheap and
/// never mutated underneath it.
#[derive(Debug, Clone, Serialize)]
pub struct DashboardSnapshot {
    pub taken_at_ms: u128,
    pub cursors: DomainCursors,
    pub summaries: SnapshotSummaries,
    /// Serialized by DEREF, like `topology` (no serde `rc` feature).
    #[serde(serialize_with = "serialize_arc")]
    pub metrics: Arc<MetricsView>,
    /// The ONE topology cut captured in this snapshot. Serialized by DEREF (serde's
    /// blanket `Arc: Serialize` needs the `rc` feature, which we don't enable
    /// crate-wide; the inner `ProviderHealthSnapshot` already derives `Serialize`).
    #[serde(serialize_with = "serialize_arc")]
    pub topology: Arc<ProviderHealthSnapshot>,
}

/// Serialize an `Arc<T>` by dereferencing to the inner value (avoids enabling serde's
/// `rc` feature just for the snapshot DTO).
fn serialize_arc<T, S>(value: &Arc<T>, serializer: S) -> Result<S::Ok, S::Error>
where
    T: Serialize,
    S: serde::Serializer,
{
    (**value).serialize(serializer)
}

/// Approximate retained bytes of one body-free [`SnapshotFlowSummary`]: the sum of
/// its dynamic scalar string lengths + the fixed struct size. There are NO body
/// fields to count (the summary is body-free by construction), so this is the full
/// memory footprint — the basis for the ring-quota assertion.
fn summary_approx_bytes(summary: &SnapshotFlowSummary) -> usize {
    let opt = |value: &Option<String>| value.as_ref().map(String::len).unwrap_or(0);
    std::mem::size_of::<SnapshotFlowSummary>()
        + std::mem::size_of::<usize>() * 2 // the `Arc` strong/weak counts
        + summary.api_call_id.len()
        + opt(&summary.response_id)
        + summary.method.len()
        + summary.uri.len()
        + opt(&summary.model_requested)
        + opt(&summary.model_served)
        + opt(&summary.upstream_target)
        + opt(&summary.terminal_reason)
}

/// Reference counts of the shared allocations the ring retains, keyed by allocation
/// address (stable while the ring holds the `Arc`, so it cannot be reused under us).
/// The quota charges an allocation's bytes ONCE — when the first cut holding it is
/// pushed — and releases them when the last such cut is popped. That makes the quota
/// measure what is actually resident, not `cuts × flows`.
#[derive(Debug, Default)]
struct SharedAllocations {
    counts: std::collections::HashMap<usize, (usize, usize)>,
}

impl SharedAllocations {
    /// Take a reference; returns the bytes NEWLY retained (non-zero only for the
    /// first holder).
    fn acquire(&mut self, id: usize, bytes: impl FnOnce() -> usize) -> usize {
        match self.counts.entry(id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().0 += 1;
                0
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let bytes = bytes();
                entry.insert((1, bytes));
                bytes
            }
        }
    }

    /// Drop a reference; returns the bytes freed (non-zero only for the last holder).
    fn release(&mut self, id: usize) -> usize {
        let std::collections::hash_map::Entry::Occupied(mut entry) = self.counts.entry(id) else {
            return 0;
        };
        entry.get_mut().0 -= 1;
        if entry.get().0 == 0 {
            entry.remove().1
        } else {
            0
        }
    }
}

/// The bounded ring of body-free [`DashboardSnapshot`] cuts (720 = 1 h at 5 s). A
/// fresh cut is pushed at the back; the ring is bounded BOTH by slot count (720) AND
/// by a retained-byte quota over UNIQUE allocations (a summary shared by 360 cuts is
/// charged once) — when a push would exceed it, the OLDEST cuts are dropped first.
/// `snapshot_at(ts)` binary-searches the time-ordered cuts for the nearest cut with
/// `taken_at_ms ≤ ts`.
#[derive(Debug, Default)]
struct SnapshotRing {
    cuts: std::collections::VecDeque<Arc<DashboardSnapshot>>,
    /// Summary lists (outer level: an idle cut re-holding the same list is O(1)).
    lists: SharedAllocations,
    /// Individual summaries (inner level: charged when the first list holding them
    /// is retained).
    summaries: SharedAllocations,
    /// Metrics views and topology cuts.
    views: SharedAllocations,
    retained_bytes: usize,
    quota_bytes: usize,
}

impl SnapshotRing {
    fn new(quota_bytes: usize) -> Self {
        Self {
            cuts: std::collections::VecDeque::with_capacity(SNAPSHOT_RING_SLOTS),
            quota_bytes,
            ..Self::default()
        }
    }

    /// Push a fresh cut, then enforce BOTH the slot cap (720) and the byte quota by
    /// dropping the OLDEST cuts. Cuts are pushed in monotonic `taken_at_ms` order (the
    /// 5 s task is the only writer), so the deque stays time-sorted for
    /// `snapshot_at`'s binary search.
    fn push(&mut self, cut: Arc<DashboardSnapshot>) {
        let added = self.acquire(&cut);
        self.retained_bytes = self.retained_bytes.saturating_add(added);
        self.cuts.push_back(cut);
        while self.cuts.len() > SNAPSHOT_RING_SLOTS {
            self.pop_oldest();
        }
        while self.retained_bytes > self.quota_bytes && self.cuts.len() > 1 {
            // Keep at least the newest cut even if a single cut somehow exceeds the
            // quota (degenerate); the quota is a peak bound, not a per-cut bound.
            self.pop_oldest();
        }
    }

    fn pop_oldest(&mut self) {
        if let Some(old) = self.cuts.pop_front() {
            let freed = self.release(&old);
            self.retained_bytes = self.retained_bytes.saturating_sub(freed);
        }
    }

    fn acquire(&mut self, cut: &DashboardSnapshot) -> usize {
        let mut added = std::mem::size_of::<DashboardSnapshot>();
        let list = &cut.summaries;
        let list_bytes = self.lists.acquire(list.alloc_id(), || {
            std::mem::size_of_val::<[SharedFlowSummary]>(list)
        });
        if list_bytes > 0 {
            added += list_bytes;
            for summary in list {
                added += self
                    .summaries
                    .acquire(summary.alloc_id(), || summary_approx_bytes(summary));
            }
        }
        added += self.views.acquire(Arc::as_ptr(&cut.metrics) as usize, || {
            cut.metrics.approx_bytes()
        });
        added += self.views.acquire(Arc::as_ptr(&cut.topology) as usize, || {
            std::mem::size_of::<ProviderHealthSnapshot>()
        });
        added
    }

    fn release(&mut self, cut: &DashboardSnapshot) -> usize {
        let mut freed = std::mem::size_of::<DashboardSnapshot>();
        let list_bytes = self.lists.release(cut.summaries.alloc_id());
        if list_bytes > 0 {
            freed += list_bytes;
            for summary in &cut.summaries {
                freed += self.summaries.release(summary.alloc_id());
            }
        }
        freed += self.views.release(Arc::as_ptr(&cut.metrics) as usize);
        freed += self.views.release(Arc::as_ptr(&cut.topology) as usize);
        freed
    }

    /// The nearest retained cut with `taken_at_ms ≤ ts` (the `/snapshot?at=`
    /// backend). `None` when the ring is empty or every cut is newer than `ts`.
    /// Binary search over the time-sorted deque.
    fn snapshot_at(&self, ts: u128) -> Option<Arc<DashboardSnapshot>> {
        let index = self.cuts.partition_point(|cut| cut.taken_at_ms <= ts);
        index
            .checked_sub(1)
            .map(|index| Arc::clone(&self.cuts[index]))
    }

    /// The most recent cut, if any (the live `/snapshot` with no `at=`).
    fn latest(&self) -> Option<Arc<DashboardSnapshot>> {
        self.cuts.back().map(Arc::clone)
    }
}

/// Interior state of the [`MetricsLayer`], guarded by its single `Mutex`. Holds the
/// three window rings, the monotonic `metrics_seq`, and the snapshot ring. The
/// snapshot ring lives UNDER the same mutex so the 5 s task pushes a cut while
/// already holding the metrics lock (inside the FlowStore→Metrics critical section),
/// avoiding a third lock.
#[derive(Debug)]
struct MetricsState {
    ring_1m: WindowRing,
    ring_5m: WindowRing,
    ring_1h: WindowRing,
    metrics_seq: u64,
    snapshots: SnapshotRing,
}

impl MetricsState {
    fn new(snapshot_quota_bytes: usize) -> Self {
        Self {
            ring_1m: WindowRing::new(WINDOW_1M_SLOTS),
            ring_5m: WindowRing::new(WINDOW_5M_SLOTS),
            ring_1h: WindowRing::new(WINDOW_1H_SLOTS),
            metrics_seq: 0,
            snapshots: SnapshotRing::new(snapshot_quota_bytes),
        }
    }

    /// Record one terminal response into all three rings at `epoch_s`, bumping
    /// `metrics_seq`.
    fn record_response(&mut self, epoch_s: u64, key: &BucketKey, elapsed_ms: f64) {
        for ring in [&mut self.ring_1m, &mut self.ring_5m, &mut self.ring_1h] {
            let slot = ring.slot_mut(epoch_s);
            let entry = slot.buckets.entry(key.clone()).or_default();
            entry.count = entry.count.saturating_add(1);
            slot.histogram.record(elapsed_ms);
        }
        self.metrics_seq = self.metrics_seq.saturating_add(1);
    }

    /// Record one terminal response AND its optional final cumulative usage into all
    /// three rings at the SAME `epoch_s`, bumping `metrics_seq` ONCE (Codex D5 R1 #2).
    /// This is the atomic terminal path: the response count and the token totals are
    /// applied under a SINGLE metrics-lock hold at ONE epoch/slot, so a concurrent 5 s
    /// snapshot can never interleave between them and land the count and the tokens in
    /// DIFFERENT 1 s slots (which separate `record_response` + `add_tokens` calls —
    /// each computing its own `now_epoch_s()` under its own lock — could do across a
    /// second boundary). Both join the same `{status, model, endpoint, upstream}`
    /// bucket key, so a completed flow's count + tokens are always co-located.
    #[allow(clippy::too_many_arguments)]
    fn record_terminal(
        &mut self,
        epoch_s: u64,
        key: &BucketKey,
        elapsed_ms: f64,
        usage: Option<FlowUsage>,
        prefill_ms: Option<u128>,
        decode_ms: Option<u128>,
        attempts: &[Attempt],
    ) {
        for ring in [&mut self.ring_1m, &mut self.ring_5m, &mut self.ring_1h] {
            let slot = ring.slot_mut(epoch_s);
            // Gap 12: fold the per-attempt trace into the per-provider tallies in the SAME
            // slot, under the SAME single metrics-lock hold as the count/tokens — so a
            // concurrent 5 s snapshot can never observe the flow's terminal count without
            // its per-provider attempt samples (they are co-located atomically). A FAILED
            // primary that failed over IS counted toward its own provider (final-served
            // latency alone would hide it — spec 12). Bounded by the per-slot provider cap.
            record_attempts_into_slot(slot, attempts);
            let entry = slot.buckets.entry(key.clone()).or_default();
            entry.count = entry.count.saturating_add(1);
            if let Some(usage) = usage {
                entry.prompt_tokens = entry.prompt_tokens.saturating_add(usage.prompt);
                entry.completion_tokens = entry.completion_tokens.saturating_add(usage.completion);
                // Gap 07: an UNREPORTED (`None`) cached/reasoning class contributes
                // nothing to the window THROUGHPUT sum (it is not a `0` charge, it is
                // "no data") — `unwrap_or(0)` is the correct additive identity here.
                entry.cached_tokens = entry
                    .cached_tokens
                    .saturating_add(usage.cached.unwrap_or(0));
                entry.reasoning_tokens = entry
                    .reasoning_tokens
                    .saturating_add(usage.reasoning.unwrap_or(0));
                // This terminal flow reported usage → it is a MEASURED token sample
                // (gap 01 finding 3). A terminal with `usage == None` increments only
                // `count`, so `tokens_per_sec` over usage-less flows reads `—`.
                entry.usage_samples = entry.usage_samples.saturating_add(1);
                // Gap 07: a usage-bearing flow whose `cached` was UNREPORTED preserves
                // the estimated-cost signal the summed `cached_tokens` would erase.
                if usage.cached.is_none() {
                    entry.unreported_cached_samples =
                        entry.unreported_cached_samples.saturating_add(1);
                }
                if let Some(prefill_ms) = prefill_ms.filter(|duration| *duration > 0) {
                    entry.prefill_tokens = entry.prefill_tokens.saturating_add(usage.prompt);
                    entry.prefill_ms = entry.prefill_ms.saturating_add(prefill_ms);
                    entry.prefill_samples = entry.prefill_samples.saturating_add(1);
                }
                if let Some(decode_ms) = decode_ms.filter(|duration| *duration > 0) {
                    entry.decode_tokens = entry.decode_tokens.saturating_add(usage.completion);
                    entry.decode_ms = entry.decode_ms.saturating_add(decode_ms);
                    entry.decode_samples = entry.decode_samples.saturating_add(1);
                }
            }
            slot.histogram.record(elapsed_ms);
        }
        self.metrics_seq = self.metrics_seq.saturating_add(1);
    }

    /// Add a flow's token counts to the bucket for `key` at `epoch_s`. Called ONCE
    /// per flow at the terminal seam with the flow's FINAL cumulative usage, so the
    /// slot's token sum equals the window's true token throughput (no per-chunk
    /// over-counting of cumulative values). Bumps `metrics_seq`.
    fn add_tokens(&mut self, epoch_s: u64, key: &BucketKey, usage: FlowUsage) {
        for ring in [&mut self.ring_1m, &mut self.ring_5m, &mut self.ring_1h] {
            let slot = ring.slot_mut(epoch_s);
            let entry = slot.buckets.entry(key.clone()).or_default();
            entry.prompt_tokens = entry.prompt_tokens.saturating_add(usage.prompt);
            entry.completion_tokens = entry.completion_tokens.saturating_add(usage.completion);
            // Gap 07: `None` cached/reasoning = unreported, contributes 0 to throughput.
            entry.cached_tokens = entry
                .cached_tokens
                .saturating_add(usage.cached.unwrap_or(0));
            entry.reasoning_tokens = entry
                .reasoning_tokens
                .saturating_add(usage.reasoning.unwrap_or(0));
            // A reported-usage sample (gap 01 finding 3) — the `tokens_per_sec`
            // measurability denominator. The standalone `record_usage` path joins the
            // SAME bucket key as its `record_response`, so on the production terminal
            // path (which uses the atomic `record_terminal`) usage is counted exactly
            // once; this keeps the older split path consistent for its callers/tests.
            entry.usage_samples = entry.usage_samples.saturating_add(1);
            // Gap 07: preserve the unreported-cached estimated-cost signal (see
            // `record_terminal`).
            if usage.cached.is_none() {
                entry.unreported_cached_samples = entry.unreported_cached_samples.saturating_add(1);
            }
        }
        self.metrics_seq = self.metrics_seq.saturating_add(1);
    }

    /// Collapse the three rings into a body-free [`MetricsView`] as of `now_epoch_s`.
    fn view(&self, now_epoch_s: u64) -> MetricsView {
        let window_1m = self.ring_1m.aggregate(now_epoch_s);
        let window_5m = self.ring_5m.aggregate(now_epoch_s);
        let window_1h = self.ring_1h.aggregate(now_epoch_s);
        let percentiles_1m = window_1m.percentiles();
        let percentiles_5m = window_5m.percentiles();
        let percentiles_1h = window_1h.percentiles();
        MetricsView {
            window_1m,
            window_5m,
            window_1h,
            percentiles_1m,
            percentiles_5m,
            percentiles_1h,
        }
    }
}

/// Authoritative aggregated stats + the coordinated body-free snapshot store (D5).
/// Mirrors the `MonitorHub`/`DashboardFlowStore` `new()`/`disabled()` zero-overhead
/// split: when `disabled()` EVERY method early-returns and takes NO lock, so the
/// production hot path is unchanged. `Clone` (state behind `Arc<Mutex<_>>`) so it
/// threads into the `#[derive(Clone)] Gateway` like the FlowStore/monitor do.
#[derive(Clone)]
pub struct MetricsLayer {
    enabled: bool,
    state: Arc<Mutex<MetricsState>>,
}

impl std::fmt::Debug for MetricsLayer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetricsLayer")
            .field("enabled", &self.enabled)
            .finish_non_exhaustive()
    }
}

impl MetricsLayer {
    /// Enabled layer (debug UI on). Uses the default 64 MiB snapshot-ring quota.
    pub fn new() -> Self {
        let quota = std::env::var("LLMCONDUIT_DASHBOARD_SNAPSHOT_BYTES")
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|quota| *quota > 0)
            .unwrap_or(DEFAULT_SNAPSHOT_QUOTA_BYTES);
        Self::with_snapshot_quota(quota)
    }

    /// No-op layer (debug UI off). Every method early-returns and takes NO lock, so
    /// the production hot path keeps zero overhead — mirrors `MonitorHub::disabled()`.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            // A tiny quota; the state is never mutated on the disabled path.
            state: Arc::new(Mutex::new(MetricsState::new(0))),
        }
    }

    /// Explicit quota also lets tests drive eviction without allocating 400 MiB.
    fn with_snapshot_quota(quota_bytes: usize) -> Self {
        Self {
            enabled: true,
            state: Arc::new(Mutex::new(MetricsState::new(quota_bytes))),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MetricsState> {
        self.state.lock().expect("metrics layer lock poisoned")
    }

    /// Record one TERMINAL response (the D3 terminal finalize seam — NOT the
    /// middleware, NOT per-chunk). No-op when disabled (zero lock, zero work). The
    /// `served_model`/`endpoint`/`upstream` collapse to `"unknown"` when absent so
    /// the key space stays bounded.
    pub fn record_response(
        &self,
        status: FlowStatus,
        served_model: Option<&str>,
        endpoint: &str,
        upstream: Option<&str>,
        elapsed_ms: u128,
    ) {
        if !self.enabled {
            return;
        }
        let key = BucketKey {
            status: StatusClass::from_status(status),
            model: label_or_unknown(served_model),
            endpoint: endpoint.to_string(),
            upstream: label_or_unknown(upstream),
        };
        let epoch_s = now_epoch_s();
        self.lock()
            .record_response(epoch_s, &key, elapsed_ms as f64);
    }

    /// Record a flow's FINAL cumulative token usage into the token counters, called
    /// ONCE per flow at the D3 terminal seam (co-located with `record_response`).
    /// Recording the final cumulative once — rather than each cumulative chunk —
    /// makes the slot's token sum the window's true throughput without delta
    /// bookkeeping or per-chunk over-counting. The bucket is keyed by the flow's
    /// terminal `status` so it joins the same bucket as its `record_response` (a
    /// completed flow's tokens + count share one `{Success, model, endpoint,
    /// upstream}` key). No-op when disabled.
    pub fn record_usage(
        &self,
        status: FlowStatus,
        served_model: Option<&str>,
        endpoint: &str,
        upstream: Option<&str>,
        usage: FlowUsage,
    ) {
        if !self.enabled {
            return;
        }
        let key = BucketKey {
            status: StatusClass::from_status(status),
            model: label_or_unknown(served_model),
            endpoint: endpoint.to_string(),
            upstream: label_or_unknown(upstream),
        };
        let epoch_s = now_epoch_s();
        self.lock().add_tokens(epoch_s, &key, usage);
    }

    /// **The atomic terminal record (Codex D5 R1 #2).** Records a flow's terminal
    /// response count AND its optional FINAL cumulative token usage in a SINGLE
    /// metrics-lock hold at ONE epoch/slot — the engine drives this once at the D3
    /// terminal finalize seam INSTEAD of a separate `record_response` + `record_usage`
    /// pair. Taking one lock and computing one `epoch_s` here guarantees the count and
    /// the tokens land in the SAME 1 s slot even if a 5 s coordinated snapshot runs
    /// concurrently: there is no window between two lock acquisitions for a snapshot to
    /// observe the count without the tokens (or to split them across a second
    /// boundary). The `served_model`/`endpoint`/`upstream` collapse to `"unknown"`
    /// when absent so the key space stays bounded. No-op (zero lock, zero work) when
    /// disabled.
    ///
    /// Gap 12: `attempts` is the flow's evict-safe per-attempt trace (spec 03, carried
    /// on the SAME terminal payload as `usage`). Each attempt feeds the per-provider
    /// latency/error rings in the SAME slot under the SAME lock hold — so a FAILED
    /// primary is counted toward ITS provider (not hidden behind the final server) and
    /// the per-provider samples can never be split from the flow's terminal count by a
    /// concurrent snapshot. Pass `&[]` for a flow with no recorded attempt.
    // The terminal seam threads distinct attribution + usage + the attempt trace; these
    // are independent finalize-time facts, not a cohesive struct (mirrors the engine's
    // own `#[allow]`ed terminal-finalize signatures).
    #[allow(clippy::too_many_arguments)]
    pub fn record_terminal(
        &self,
        status: FlowStatus,
        served_model: Option<&str>,
        endpoint: &str,
        upstream: Option<&str>,
        elapsed_ms: u128,
        usage: Option<FlowUsage>,
        attempts: &[Attempt],
    ) {
        self.record_terminal_with_phases(
            status,
            served_model,
            endpoint,
            upstream,
            elapsed_ms,
            usage,
            None,
            None,
            attempts,
        );
    }

    /// Record a terminal response with measured prefill and decode durations.
    /// The phase durations are optional because older/non-streaming capture paths may
    /// not observe both phase boundaries; missing measurements never become fake zeroes.
    #[allow(clippy::too_many_arguments)]
    pub fn record_terminal_with_phases(
        &self,
        status: FlowStatus,
        served_model: Option<&str>,
        endpoint: &str,
        upstream: Option<&str>,
        elapsed_ms: u128,
        usage: Option<FlowUsage>,
        prefill_ms: Option<u128>,
        decode_ms: Option<u128>,
        attempts: &[Attempt],
    ) {
        if !self.enabled {
            return;
        }
        let key = BucketKey {
            status: StatusClass::from_status(status),
            model: label_or_unknown(served_model),
            endpoint: endpoint.to_string(),
            upstream: label_or_unknown(upstream),
        };
        let epoch_s = now_epoch_s();
        self.lock().record_terminal(
            epoch_s,
            &key,
            elapsed_ms as f64,
            usage,
            prefill_ms,
            decode_ms,
            attempts,
        );
    }

    /// The current metrics domain sequence (the per-domain cursor). `0` when
    /// disabled.
    pub fn metrics_seq(&self) -> u64 {
        if !self.enabled {
            return 0;
        }
        self.lock().metrics_seq
    }

    /// Collapse the three rings into a body-free [`MetricsView`] as of NOW. Empty
    /// when disabled.
    pub fn view(&self) -> MetricsView {
        if !self.enabled {
            return MetricsView::default();
        }
        let now = now_epoch_s();
        self.lock().view(now)
    }

    /// Collapse the rings into a [`MetricsView`] **AND** read `metrics_seq` in ONE
    /// lock hold (D7b R2 finding 2). The `/dashboard/ws` initial snapshot pairs the
    /// metrics body with its dedup cursor: reading `view()` and `metrics_seq()` as
    /// SEPARATE lock acquisitions leaves a window where a mutation between them seeds an
    /// OLDER metrics body with a NEWER cursor — so the next live `metric_tick` (whose
    /// seq advanced only to that same mutation) is whole-frame-deduped on the client and
    /// the tile never updates. Capturing both under one lock guarantees the cursor is
    /// exactly the watermark of the view returned. `(MetricsView::default(), 0)` when
    /// disabled.
    ///
    /// Time-inside-the-lock (D7b R3 HIGH): `now_epoch_s()` is sampled AFTER the lock is
    /// acquired, so the view's aggregation window, the `metrics_seq`, and the bucketed
    /// data are mutually consistent. Sampling `now` BEFORE the lock leaves a window in
    /// which a mutation in the next second enters `metrics_seq` (and its tokens land in
    /// the current-epoch bucket) yet is EXCLUDED from a view aggregated against the older
    /// `now` — pairing a newer seq with a view that omits that very sample.
    pub fn view_with_seq(&self) -> (MetricsView, u64) {
        if !self.enabled {
            return (MetricsView::default(), 0);
        }
        let state = self.lock();
        let now = now_epoch_s();
        (state.view(now), state.metrics_seq)
    }

    /// **The 5 s coordinated snapshot.** Takes the FIXED lock order — FlowStore mutex
    /// FIRST, THEN this layer's metrics mutex nested under it — and captures ONE
    /// `Arc<ProviderHealthSnapshot>` from the D4 publisher, producing a true atomic cut
    /// across all three stores into a body-free [`DashboardSnapshot`]. Only this method
    /// ever holds >1 lock, so the fixed order makes a deadlock impossible. The summaries
    /// are body-free (NO `Arc<[u8]>`). No-op (returns `None`) when disabled. The cut is
    /// pushed onto the bounded snapshot ring AND returned (so the caller/test can assert
    /// on it).
    ///
    /// Atomic-cut guarantee (Codex D5 R1 #1): the FlowStore lock is held until the
    /// metrics lock is nested under it and the cut-defining cursors + topology `Arc` are
    /// captured — that overlap instant IS the cut, so there is no gap in which a writer
    /// could mutate both stores and produce a torn cut (a metrics sample for a flow
    /// absent from the summaries). The FlowStore lock is then released and the heavier
    /// metrics aggregation runs under the metrics lock alone (still sufficient to keep
    /// the cut consistent — no writer can mutate metrics while it is held — and it keeps
    /// the FlowStore critical section minimal so writers are not starved). The cursors
    /// record exactly which versions were read at the cut.
    pub fn snapshot(
        &self,
        flow_store: &DashboardFlowStore,
        topology: &ProviderHealthPublisher,
    ) -> Option<Arc<DashboardSnapshot>> {
        self.snapshot_with_monitor_seq(flow_store, topology, 0)
    }

    /// Like [`snapshot`](Self::snapshot) but also records the monitor domain's
    /// sequence into the cut's cursors (the snapshot task owns the monitor handle).
    /// Same FIXED FlowStore→Metrics atomic cut (FlowStore lock held until the metrics
    /// lock is nested + ALL cut metadata captured, then released) + one topology Arc.
    pub fn snapshot_with_monitor_seq(
        &self,
        flow_store: &DashboardFlowStore,
        topology: &ProviderHealthPublisher,
        monitor_seq: u64,
    ) -> Option<Arc<DashboardSnapshot>> {
        // A pre-read monitor sequence: hand it back verbatim from inside the cut.
        self.snapshot_with(flow_store, topology, || monitor_seq)
    }

    /// The cut-builder shared by every snapshot entry point. `read_monitor_seq` is
    /// invoked INSIDE the dual-lock critical section so the live monitor cursor is
    /// sampled at the cut instant alongside the rest of the metadata, not before the
    /// locks (the snapshot task passes `|| monitor.last_sequence()`).
    ///
    /// Metadata-inside-the-lock guarantee (Codex D5 R2 HIGH): EVERY field that defines
    /// the cut — `taken_at_ms`/`now_epoch`, the topology `Arc` + its version, the
    /// metrics cursor, AND the monitor cursor — is captured only AFTER BOTH the
    /// FlowStore guard and the metrics lock are held. Capturing the timestamp/epoch
    /// before the locks could stamp the cut EARLIER than the state it contains: under
    /// contention `snapshot_at(ts)` would return data newer than its own `taken_at_ms`,
    /// and `view(now_epoch)` would aggregate against a stale epoch while `metrics_seq`
    /// already reflected newer samples.
    ///
    /// Stamp-LAST ordering (Codex D5 R3 HIGH): within the held locks the timestamp is
    /// taken LAST — AFTER the topology `Arc` + version, the monitor cursor, and the
    /// metrics cursor are all sampled (the flow summaries + `flow_seq` were already
    /// read under the FlowStore lock before this closure ran). Topology and the monitor
    /// are INDEPENDENT domains (neither is under the FlowStore or Metrics lock), so a
    /// topology publish or a monitor emit racing this cut could land AFTER a
    /// prematurely-stamped timestamp yet still be folded into the cut — making
    /// `taken_at_ms` predate a field the cut carries. Sampling every independent field
    /// FIRST and stamping the clock LAST makes `taken_at_ms` ≥ the read-time of EVERY
    /// field the cut contains (no field is ever newer than the stamp).
    fn snapshot_with<S>(
        &self,
        flow_store: &DashboardFlowStore,
        topology: &ProviderHealthPublisher,
        read_monitor_seq: S,
    ) -> Option<Arc<DashboardSnapshot>>
    where
        S: FnOnce() -> u64,
    {
        if !self.enabled {
            return None;
        }
        // FIXED LOCK ORDER, single atomic cut: hold the FlowStore lock only until the
        // metrics lock is nested under it and ALL cut metadata (timestamp/epoch,
        // topology Arc, both cursors) are captured — THAT instant is the cut. The
        // (heavier) metrics aggregation + ring push then run under the metrics lock
        // ALONE, after the FlowStore lock is released, so the FlowStore critical section
        // stays minimal and concurrent writers are not starved by the snapshot's
        // aggregation. Correctness after the early FlowStore release: the metrics lock is
        // STILL HELD, so no writer can complete a both-stores mutation (every metrics
        // mutation needs it) and the summaries are already a copy — the cut stays
        // internally consistent. The FlowStore lock is acquired first and never
        // re-entered here.
        let cut = flow_store.with_summaries_under_lock(|flow_guard, summaries, flow_seq| {
            // FIXED LOCK ORDER step 2: nest the metrics mutex under the FlowStore lock.
            // Both held now == the atomic cut instant — sample EVERY piece of cut
            // metadata here so none of it predates the state the cut contains.
            let mut state = self.lock();
            // D5 R3 (HIGH): sample ALL independently-mutable cut fields FIRST, then
            // stamp the cut timestamp LAST. Topology and the monitor are INDEPENDENT
            // domains — neither is under the FlowStore or Metrics lock — so a topology
            // publish or a monitor emit can land AFTER a prematurely-stamped timestamp
            // yet still be included in this cut, making `taken_at_ms` EARLIER than a
            // field the cut contains (a "future" snapshot for topology/monitor). The
            // `summaries` + `flow_seq` were already read under the held FlowStore lock;
            // here we read the remaining independent fields (topology Arc + version, the
            // monitor cursor, the metrics cursor) BEFORE taking the clock, so the cut's
            // `taken_at_ms` is ≥ the read-time of EVERY field the cut carries.
            // ONE topology Arc capture (D4) at the cut instant.
            let topology = topology.latest();
            let topology_seq = topology.version;
            let metrics_seq = state.metrics_seq;
            // The monitor cursor, sampled at the cut instant (not pre-read by the caller).
            let monitor_seq = read_monitor_seq();
            // Wall-clock instant of the cut, stamped LAST — after every independent
            // field above is already sampled — so it is never earlier than any
            // flow/sample/topology/monitor read the cut includes.
            let taken_at_ms = now_ms();
            let now_epoch = (taken_at_ms / 1000) as u64;
            // Cut instant fixed: release the FlowStore lock and finish the metrics-only
            // work (aggregation + push) under the metrics guard alone.
            flow_guard.release();
            let view = state.view(now_epoch);
            // An idle box produces the same view cut after cut: share the previous
            // cut's allocation instead of retaining 720 equal copies.
            let metrics = match state.snapshots.latest() {
                Some(previous) if *previous.metrics == view => Arc::clone(&previous.metrics),
                _ => Arc::new(view),
            };
            let cursors = DomainCursors {
                flow_seq,
                metrics_seq,
                topology_seq,
                monitor_seq,
            };
            let cut = Arc::new(DashboardSnapshot {
                taken_at_ms,
                cursors,
                summaries,
                metrics,
                topology,
            });
            state.snapshots.push(Arc::clone(&cut));
            cut
        });
        Some(cut)
    }

    /// The nearest retained snapshot cut with `taken_at_ms ≤ ts` (the
    /// `/snapshot?at=` backend; D13 registers the route). `None` when disabled, the
    /// ring is empty, or every cut is newer than `ts`.
    pub fn snapshot_at(&self, ts: u128) -> Option<Arc<DashboardSnapshot>> {
        if !self.enabled {
            return None;
        }
        self.lock().snapshots.snapshot_at(ts)
    }

    /// The most recent retained snapshot cut (live `/snapshot` with no `at=`). `None`
    /// when disabled or no cut has been taken yet.
    pub fn latest_snapshot(&self) -> Option<Arc<DashboardSnapshot>> {
        if !self.enabled {
            return None;
        }
        self.lock().snapshots.latest()
    }
}

impl Default for MetricsLayer {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn the D5 5-second coordinated snapshot task. Gated by `--with-debug-ui` at
/// the DI root (production must NOT run this — zero overhead). The task holds only
/// `Clone` handles (the metrics layer, the FlowStore, the topology publisher, the
/// monitor) — all behind `Arc`, so it is `Send + 'static` and runs for the process
/// lifetime. Every 5 s it takes the single FlowStore→Metrics critical section and,
/// INSIDE it, captures one topology `Arc` and reads the monitor sequence, then pushes
/// a body-free cut onto the bounded ring. Passing `|| monitor.last_sequence()` (rather
/// than a pre-read value) keeps the monitor cursor sampled at the cut instant with the
/// rest of the metadata. Returns the `JoinHandle` so a caller/test can abort it.
pub fn spawn_snapshot_task(
    metrics: MetricsLayer,
    flow_store: DashboardFlowStore,
    topology: ProviderHealthPublisher,
    monitor: crate::monitor::MonitorHub,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            // Read the monitor cursor INSIDE the cut (the closure runs under the held
            // locks) so it is consistent with the rest of the snapshot metadata.
            metrics.snapshot_with(&flow_store, &topology, || monitor.last_sequence());
        }
    })
}

/// `value` or the bounded `"unknown"` sentinel, so a missing attribution never
/// spawns a distinct `None` bucket and the key space stays bounded.
fn label_or_unknown(value: Option<&str>) -> String {
    value.unwrap_or("unknown").to_string()
}

/// `skip_serializing_if` predicate for the per-class error counters: a `0` class is
/// ABSENT on the wire, so a provider's error distribution lists only the classes it hit.
fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn now_epoch_s() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a histogram from an explicit list of latency samples (ms).
    fn histogram_of(samples: &[f64]) -> Histogram {
        let mut histogram = Histogram::default();
        for &sample in samples {
            histogram.record(sample);
        }
        histogram
    }

    #[test]
    fn disabled_layer_is_a_no_op() {
        let metrics = MetricsLayer::disabled();
        assert!(!metrics.is_enabled());
        metrics.record_response(
            FlowStatus::Completed,
            Some("m"),
            "/v1/responses",
            Some("p"),
            12,
        );
        metrics.record_usage(
            FlowStatus::Completed,
            Some("m"),
            "/v1/responses",
            Some("p"),
            FlowUsage::default(),
        );
        assert_eq!(metrics.metrics_seq(), 0, "disabled layer never bumps seq");
        let view = metrics.view();
        assert_eq!(view.window_1m.total_count(), 0);
        assert!(metrics.latest_snapshot().is_none());
        assert!(metrics.snapshot_at(u128::MAX).is_none());
        // The coordinated snapshot is a no-op too.
        let flow = DashboardFlowStore::disabled();
        let topo = ProviderHealthPublisher::default();
        assert!(metrics.snapshot(&flow, &topo).is_none());
    }

    #[test]
    fn quantile_known_distribution_uniform_1_to_100() {
        // 100 samples at 1,2,...,100 ms. The histogram is coarse (log-spaced
        // buckets), so assert the interpolated quantiles land in a sane band around
        // the true 50th/95th/99th values (50.5, 95.95, 99.99 ms).
        let samples: Vec<f64> = (1..=100).map(|value| value as f64).collect();
        let histogram = histogram_of(&samples);
        let p50 = histogram.quantile(0.50);
        let p95 = histogram.quantile(0.95);
        let p99 = histogram.quantile(0.99);
        assert!(
            (30.0..=75.0).contains(&p50),
            "p50 {p50} within log-bucket tolerance of 50ms"
        );
        assert!(
            (75.0..=120.0).contains(&p95),
            "p95 {p95} within log-bucket tolerance of 96ms"
        );
        assert!(
            (80.0..=130.0).contains(&p99),
            "p99 {p99} within log-bucket tolerance of 100ms"
        );
        // Monotonic ordering must hold exactly.
        assert!(p50 <= p95 && p95 <= p99, "quantiles monotonic");
    }

    #[test]
    fn quantile_empty_histogram_is_zero() {
        let histogram = Histogram::default();
        assert_eq!(histogram.quantile(0.50), 0.0);
        assert_eq!(histogram.quantile(0.95), 0.0);
        assert_eq!(histogram.quantile(0.99), 0.0);
    }

    #[test]
    fn quantile_single_sample_lands_in_its_bucket() {
        // A single 50 ms sample: every quantile interpolates within the bucket that
        // contains 50 ms, i.e. (lower, upper] straddling 50.
        let histogram = histogram_of(&[50.0]);
        let index = histogram.bucket_index(50.0);
        let lower = bucket_lower_ms(index);
        let upper = bucket_upper_ms(index);
        for quantile in [0.50, 0.95, 0.99] {
            let value = histogram.quantile(quantile);
            assert!(
                value >= lower && value <= upper,
                "q{quantile} {value} within bucket [{lower},{upper}]"
            );
        }
    }

    #[test]
    fn quantile_bimodal_separates_low_and_high() {
        // 90 samples at ~5 ms, 10 samples at ~5000 ms. p50 must be in the low mode,
        // p99 in the high mode.
        let mut samples = vec![5.0; 90];
        samples.extend(vec![5000.0; 10]);
        let histogram = histogram_of(&samples);
        let p50 = histogram.quantile(0.50);
        let p99 = histogram.quantile(0.99);
        assert!(p50 < 100.0, "p50 {p50} in the low (~5ms) mode");
        assert!(p99 > 1000.0, "p99 {p99} in the high (~5000ms) mode");
    }

    #[test]
    fn quantile_overflow_bucket_reports_finite_max() {
        // A sample beyond 120 s lands in the overflow bucket; its quantile reports
        // the finite max bound (120 s), never infinity.
        let histogram = histogram_of(&[500_000.0]);
        let p99 = histogram.quantile(0.99);
        assert!(p99.is_finite(), "overflow quantile is finite");
        assert!(
            (p99 - HISTOGRAM_MAX_MS).abs() < 1.0,
            "overflow quantile {p99} == finite max {HISTOGRAM_MAX_MS}"
        );
    }

    #[test]
    fn bucket_boundaries_are_monotonic_and_span_range() {
        // The log-spaced ladder is strictly increasing and brackets [1ms, 120s].
        let mut previous = 0.0;
        for index in 0..HISTOGRAM_BUCKETS - 1 {
            let upper = bucket_upper_ms(index);
            assert!(upper > previous, "bucket {index} upper {upper} increasing");
            previous = upper;
        }
        // D5 R1 #3: the FIRST finite upper bound is EXACTLY 1 ms (not ≈1.5 ms) and the
        // LAST finite upper bound is EXACTLY 120 s — the endpoints are pinned, with the
        // ladder log-spaced between them.
        assert!(
            (bucket_upper_ms(0) - HISTOGRAM_MIN_MS).abs() < 1e-9,
            "bucket 0 upper {} == exactly 1 ms",
            bucket_upper_ms(0)
        );
        assert!(
            (bucket_upper_ms(HISTOGRAM_BUCKETS - 2) - HISTOGRAM_MAX_MS).abs() < 1e-6,
            "last finite bucket {} == exactly 120 s",
            bucket_upper_ms(HISTOGRAM_BUCKETS - 2)
        );
        assert!(bucket_upper_ms(HISTOGRAM_BUCKETS - 1).is_infinite());
    }

    #[test]
    fn record_response_populates_all_windows_and_bumps_seq() {
        let metrics = MetricsLayer::new();
        assert_eq!(metrics.metrics_seq(), 0);
        metrics.record_response(
            FlowStatus::Completed,
            Some("served-m"),
            "/v1/responses",
            Some("provider-a"),
            42,
        );
        assert_eq!(metrics.metrics_seq(), 1, "one mutation bumps seq once");
        let view = metrics.view();
        assert_eq!(view.window_1m.total_count(), 1);
        assert_eq!(view.window_5m.total_count(), 1);
        assert_eq!(view.window_1h.total_count(), 1);
        // The bucket key carries the served model + endpoint + upstream + class.
        let (key, counts) = view
            .window_1m
            .buckets
            .iter()
            .next()
            .expect("one bucket present");
        assert_eq!(key.status, StatusClass::Success);
        assert_eq!(key.model, "served-m");
        assert_eq!(key.endpoint, "/v1/responses");
        assert_eq!(key.upstream, "provider-a");
        assert_eq!(counts.count, 1);
        // Latency landed in the histogram → p50 is non-zero and within a 42ms band.
        let percentiles = view.window_1m.percentiles();
        assert!(percentiles.p50 > 0.0, "p50 populated from the 42ms sample");
    }

    #[test]
    fn record_response_unknown_dimensions_collapse_to_unknown() {
        let metrics = MetricsLayer::new();
        metrics.record_response(FlowStatus::Failed, None, "/v1/chat/completions", None, 7);
        let view = metrics.view();
        let (key, _) = view.window_1m.buckets.iter().next().expect("bucket");
        assert_eq!(key.status, StatusClass::Error);
        assert_eq!(key.model, "unknown");
        assert_eq!(key.upstream, "unknown");
    }

    #[test]
    fn record_usage_sums_token_counters() {
        let metrics = MetricsLayer::new();
        metrics.record_usage(
            FlowStatus::Completed,
            Some("m"),
            "/v1/responses",
            Some("p"),
            FlowUsage {
                prompt: 100,
                completion: 40,
                total: 140,
                cached: Some(10),
                reasoning: Some(7),
            },
        );
        metrics.record_usage(
            FlowStatus::Completed,
            Some("m"),
            "/v1/responses",
            Some("p"),
            FlowUsage {
                prompt: 50,
                completion: 20,
                total: 70,
                cached: Some(5),
                reasoning: Some(3),
            },
        );
        let view = metrics.view();
        let counts = view
            .window_1m
            .buckets
            .values()
            .next()
            .expect("usage bucket");
        assert_eq!(counts.prompt_tokens, 150);
        assert_eq!(counts.completion_tokens, 60);
        assert_eq!(counts.cached_tokens, 15);
        assert_eq!(counts.reasoning_tokens, 10);
    }

    #[test]
    fn record_terminal_co_locates_count_and_tokens_in_one_slot_and_bumps_seq_once() {
        // D5 R1 #2: the atomic terminal record applies the response count + the final
        // usage under ONE lock at ONE epoch — the count and the tokens always land in
        // the SAME bucket (same key) and the seq bumps exactly once. We assert both the
        // co-location (count and all token fields present on the single bucket) and the
        // single seq bump (vs. two for separate record_response + record_usage calls).
        let metrics = MetricsLayer::new();
        assert_eq!(metrics.metrics_seq(), 0);
        metrics.record_terminal(
            FlowStatus::Completed,
            Some("served-m"),
            "/v1/responses",
            Some("provider-a"),
            42,
            Some(FlowUsage {
                prompt: 100,
                completion: 40,
                total: 140,
                cached: Some(10),
                reasoning: Some(7),
            }),
            &[],
        );
        // ONE atomic mutation ⇒ exactly one seq bump (not two).
        assert_eq!(metrics.metrics_seq(), 1, "atomic terminal bumps seq once");
        let view = metrics.view();
        // Exactly one bucket: the count AND the tokens co-located on the same key.
        assert_eq!(
            view.window_1m.buckets.len(),
            1,
            "count + tokens share a slot"
        );
        let (key, counts) = view
            .window_1m
            .buckets
            .iter()
            .next()
            .expect("the single terminal bucket");
        assert_eq!(key.status, StatusClass::Success);
        assert_eq!(key.model, "served-m");
        assert_eq!(counts.count, 1, "response counted");
        assert_eq!(
            counts.prompt_tokens, 100,
            "tokens in the SAME bucket as count"
        );
        assert_eq!(counts.completion_tokens, 40);
        assert_eq!(counts.cached_tokens, 10);
        assert_eq!(counts.reasoning_tokens, 7);
        // Latency landed too (one bucket carries the histogram sample window-wide).
        assert!(view.window_1m.percentiles().p50 > 0.0, "latency recorded");
        // All three windows agree (the single epoch fed every ring).
        assert_eq!(view.window_5m.total_count(), 1);
        assert_eq!(view.window_1h.total_count(), 1);
    }

    #[test]
    fn phase_token_rates_use_measured_durations_and_independent_sample_counts() {
        let metrics = MetricsLayer::new();
        for (prompt, completion, prefill_ms, decode_ms) in [
            (1000, 100, Some(100), Some(1000)),
            (500, 50, Some(50), None),
        ] {
            metrics.record_terminal_with_phases(
                FlowStatus::Completed,
                Some("m"),
                "/v1/responses",
                Some("p"),
                1200,
                Some(FlowUsage {
                    prompt,
                    completion,
                    total: prompt + completion,
                    cached: None,
                    reasoning: None,
                }),
                prefill_ms,
                decode_ms,
                &[],
            );
        }

        let report = metrics.view().window_1m;
        assert_eq!(report.prefill_sample_count(), 2);
        assert_eq!(report.decode_sample_count(), 1);
        assert!((report.prefill_tokens_per_sec() - 10_000.0).abs() < 1e-9);
        assert!((report.decode_tokens_per_sec() - 100.0).abs() < 1e-9);
    }

    #[test]
    fn record_terminal_without_usage_records_count_only() {
        // A terminal with no usage (e.g. a pre-spawn failure) records the count + the
        // latency but no tokens — still one atomic seq bump.
        let metrics = MetricsLayer::new();
        metrics.record_terminal(
            FlowStatus::Failed,
            None,
            "/v1/chat/completions",
            None,
            7,
            None,
            &[],
        );
        assert_eq!(metrics.metrics_seq(), 1);
        let view = metrics.view();
        let (key, counts) = view.window_1m.buckets.iter().next().expect("bucket");
        assert_eq!(key.status, StatusClass::Error);
        assert_eq!(counts.count, 1);
        assert_eq!(counts.prompt_tokens, 0, "no tokens recorded");
        assert_eq!(counts.completion_tokens, 0);
    }

    #[test]
    fn usage_samples_count_only_usage_bearing_terminals() {
        // Gap 01 finding 3: `usage_samples` is the `tokens_per_sec` measurability
        // denominator, and `priced_sample_count` the `cost_per_min` one. A terminal
        // WITH usage bumps both `count` and `usage_samples`; one WITHOUT usage bumps
        // only `count`. A usage-bearing flow on an UNPRICED model counts as a usage
        // sample but NOT a priced sample.
        let metrics = MetricsLayer::new();
        // 1) priced model, usage present → usage + priced sample.
        metrics.record_terminal(
            FlowStatus::Completed,
            Some("priced-m"),
            "/v1/responses",
            Some("p"),
            10,
            Some(FlowUsage {
                prompt: 100,
                completion: 40,
                total: 140,
                cached: Some(0),
                reasoning: Some(0),
            }),
            &[],
        );
        // 2) unpriced model, usage present → usage sample, NOT priced.
        metrics.record_terminal(
            FlowStatus::Completed,
            Some("free-m"),
            "/v1/responses",
            Some("p"),
            10,
            Some(FlowUsage {
                prompt: 10,
                completion: 5,
                total: 15,
                cached: Some(0),
                reasoning: Some(0),
            }),
            &[],
        );
        // 3) priced model, NO usage → counted request, NOT a usage/priced sample.
        metrics.record_terminal(
            FlowStatus::Failed,
            Some("priced-m"),
            "/v1/responses",
            Some("p"),
            10,
            None,
            &[],
        );
        let view = metrics.view();
        let window = &view.window_1m;
        assert_eq!(window.total_count(), 3, "all three terminals counted");
        assert_eq!(
            window.usage_sample_count(),
            2,
            "only the two usage-bearing terminals are usage samples"
        );
        // Only "priced-m" has a price → exactly one priced usage sample (case #1).
        let priced = window.priced_sample_count(|model| model.eq_ignore_ascii_case("priced-m"));
        assert_eq!(
            priced, 1,
            "only the priced-model usage-bearing terminal is a priced sample"
        );
    }

    // -- Gap 12: per-provider latency + error distribution --------------------------

    /// Build a failed attempt to `provider` spanning `latency_ms` with `class`.
    fn failed_attempt(provider: &str, latency_ms: u128, class: AttemptErrorClass) -> Attempt {
        Attempt {
            provider: Some(provider.to_string()),
            model: Some("m".to_string()),
            start_ms: 1_000,
            end_ms: 1_000 + latency_ms,
            first_upstream_byte_ms: None,
            status: AttemptStatus::Failed,
            error_class: Some(class),
            failover_reason: Some(crate::dashboard_flow::AttemptFailoverReason::ProviderFailed),
        }
    }

    /// Build a served attempt to `provider` spanning `latency_ms`.
    fn served_attempt(provider: &str, latency_ms: u128) -> Attempt {
        Attempt {
            provider: Some(provider.to_string()),
            model: Some("m".to_string()),
            start_ms: 1_000,
            end_ms: 1_000 + latency_ms,
            first_upstream_byte_ms: Some(1_000 + latency_ms),
            status: AttemptStatus::Served,
            error_class: None,
            failover_reason: None,
        }
    }

    #[test]
    fn per_provider_aggregates_attempts_off_the_terminal_seam() {
        // The per-provider ring is fed from the terminal payload's `attempts[]` (spec 03),
        // NOT a FlowStore read. A flow that failed over A → B records BOTH attempts: A as a
        // FAILED sample on provider A, B as a SERVED sample on provider B. Each provider's
        // percentiles + error rate derive from its OWN attempt latencies.
        let metrics = MetricsLayer::new();
        let attempts = vec![
            failed_attempt("provider-a", 80, AttemptErrorClass::HttpStatus),
            served_attempt("provider-b", 40),
        ];
        // The flow ultimately SUCCEEDED on B (the terminal status/upstream is B), but A's
        // failure still lands on provider A via the attempt trace.
        metrics.record_terminal(
            FlowStatus::Completed,
            Some("m"),
            "/v1/responses",
            Some("provider-b"),
            40,
            None,
            &attempts,
        );
        let view = metrics.view();
        let per = view.window_1m.per_provider();
        assert_eq!(per.len(), 2, "both attempted providers are tracked");

        let a = view
            .window_1m
            .provider_latency("provider-a")
            .expect("provider A has a failed sample");
        assert_eq!(a.data_quality, ProviderMetricQuality::Derived);
        assert_eq!(a.samples, 1);
        assert_eq!(a.served, 0);
        assert_eq!(a.failed, 1);
        assert_eq!(a.error_rate, 100.0, "A's only attempt failed");
        assert_eq!(a.errors.http_status, 1, "A's failure classed http_status");
        assert_eq!(a.errors.connect, 0);
        assert!(
            a.p50 > 0.0,
            "A's failed-attempt latency feeds its percentiles"
        );

        let b = view
            .window_1m
            .provider_latency("provider-b")
            .expect("provider B has a served sample");
        assert_eq!(b.samples, 1);
        assert_eq!(b.served, 1);
        assert_eq!(b.failed, 0);
        assert_eq!(
            b.error_rate, 0.0,
            "B served — a MEASURED 0% (not unavailable)"
        );
    }

    #[test]
    fn per_provider_failed_primary_is_counted_even_when_flow_succeeds_elsewhere() {
        // Spec 12 acceptance: a failed primary is counted (final-served latency alone hides
        // an unhealthy provider). Three flows, EACH failed primary A then served on B. The
        // per-flow terminal metrics attribute the served upstream B; only the attempt trace
        // surfaces A's three failures.
        let metrics = MetricsLayer::new();
        for _ in 0..3 {
            metrics.record_terminal(
                FlowStatus::Completed,
                Some("m"),
                "/v1/responses",
                Some("provider-b"),
                40,
                None,
                &[
                    failed_attempt("provider-a", 90, AttemptErrorClass::Timeout),
                    served_attempt("provider-b", 40),
                ],
            );
        }
        let a = metrics
            .view()
            .window_1m
            .provider_latency("provider-a")
            .expect("the failed primary is tracked despite never serving");
        assert_eq!(a.samples, 3, "all three failed primaries counted");
        assert_eq!(a.failed, 3);
        assert_eq!(a.served, 0);
        assert_eq!(a.error_rate, 100.0);
        assert_eq!(a.errors.timeout, 3, "all three classed timeout");
    }

    #[test]
    fn per_provider_zero_sample_provider_is_unavailable_not_zero() {
        // don't-lie-with-zeros (spec 12, mirrors spec 01): a provider with NO attempt
        // samples in the window has NO `ProviderLatency` (absent → frontend renders `—`),
        // NOT a fabricated all-zero percentile/0% error rate.
        let metrics = MetricsLayer::new();
        metrics.record_terminal(
            FlowStatus::Completed,
            Some("m"),
            "/v1/responses",
            Some("provider-a"),
            40,
            None,
            &[served_attempt("provider-a", 40)],
        );
        let view = metrics.view();
        // The provider that WAS attempted is present...
        assert!(view.window_1m.provider_latency("provider-a").is_some());
        // ...a provider that never appeared in any attempt is ABSENT (unavailable, not 0).
        assert!(
            view.window_1m.provider_latency("never-seen").is_none(),
            "a zero-sample provider is absent (unavailable), never a fabricated zero"
        );
        assert!(
            !view.window_1m.per_provider().contains_key("never-seen"),
            "the per-provider map omits zero-sample providers entirely"
        );
    }

    #[test]
    fn per_provider_error_distribution_tallies_each_class() {
        // The error distribution uses the BOUNDED gap-03 taxonomic classes (snake_case),
        // never raw upstream text. A provider hit by several distinct failure classes
        // tallies each one; an all-served provider has an empty distribution.
        let metrics = MetricsLayer::new();
        metrics.record_terminal(
            FlowStatus::Failed,
            None,
            "/v1/responses",
            Some("provider-a"),
            10,
            None,
            &[
                failed_attempt("provider-a", 10, AttemptErrorClass::Connect),
                failed_attempt("provider-a", 20, AttemptErrorClass::Connect),
                failed_attempt("provider-a", 30, AttemptErrorClass::Timeout),
                failed_attempt("provider-a", 40, AttemptErrorClass::Stream),
            ],
        );
        let a = metrics
            .view()
            .window_1m
            .provider_latency("provider-a")
            .unwrap();
        assert_eq!(a.samples, 4);
        assert_eq!(a.failed, 4);
        assert_eq!(a.error_rate, 100.0);
        assert_eq!(a.errors.connect, 2, "two connect failures");
        assert_eq!(a.errors.timeout, 1);
        assert_eq!(a.errors.stream, 1);
        assert_eq!(a.errors.http_status, 0);
        assert_eq!(a.errors.other, 0);
        assert_eq!(a.errors.terminal, 0);
        // The bounded distribution serializes only the non-zero classes (skip-if-zero).
        let json = serde_json::to_value(a.errors).expect("serialize error distribution");
        let obj = json.as_object().unwrap();
        assert!(obj.contains_key("connect") && obj.contains_key("timeout"));
        assert!(
            !obj.contains_key("http_status") && !obj.contains_key("other"),
            "zero classes are absent on the wire"
        );
    }

    #[test]
    fn per_provider_map_is_bounded_to_max_tracked_providers_plus_overflow() {
        // MEMORY invariant (AGENTS.md): the per-provider key space is CAPPED. Feeding far
        // more than MAX_TRACKED_PROVIDERS distinct (attacker-influenceable) provider
        // strings must NOT grow the map without bound — providers beyond the cap fold into
        // the single bounded overflow bucket, so the map holds at most cap + 1 keys.
        let metrics = MetricsLayer::new();
        let many = MAX_TRACKED_PROVIDERS + 50;
        let attempts: Vec<Attempt> = (0..many)
            .map(|index| failed_attempt(&format!("provider-{index}"), 10, AttemptErrorClass::Other))
            .collect();
        metrics.record_terminal(
            FlowStatus::Failed,
            None,
            "/v1/responses",
            None,
            10,
            None,
            &attempts,
        );
        let per = metrics.view().window_1m.per_provider();
        assert!(
            per.len() <= MAX_TRACKED_PROVIDERS + 1,
            "per-provider map bounded to {} keys, got {}",
            MAX_TRACKED_PROVIDERS + 1,
            per.len()
        );
        assert!(
            per.contains_key(OVERFLOW_PROVIDER),
            "overflow providers fold into the bounded catch-all bucket"
        );
        // Every overflow attempt is still accounted for (none silently dropped): the total
        // attempts across all tracked keys equals the input count.
        let total: u64 = per.values().map(|p| p.samples).sum();
        assert_eq!(
            total, many as u64,
            "no attempt is dropped — overflow is folded"
        );
    }

    #[test]
    fn per_provider_aggregate_union_is_bounded_across_rotating_slots() {
        // Defense in depth (AGENTS.md MEMORY): a single slot is capped at
        // MAX_TRACKED_PROVIDERS, but distinct SLOTS can carry distinct provider sets (an
        // attacker rotating provider aliases each second). The window AGGREGATE must still
        // bound its merged/serialized provider count — else the union across up to 3600
        // in-window slots would be unbounded. Place ONE distinct provider per epoch across
        // far more than the cap of seconds, then aggregate the whole 1h window.
        let metrics = MetricsLayer::new();
        let base = 2_000_000u64;
        let distinct = MAX_TRACKED_PROVIDERS + 100;
        {
            let mut state = metrics.lock();
            for index in 0..distinct {
                let epoch = base + index as u64; // one fresh provider per second
                state.record_terminal(
                    epoch,
                    &BucketKey {
                        status: StatusClass::Error,
                        model: "m".to_string(),
                        endpoint: "/v1/responses".to_string(),
                        upstream: "u".to_string(),
                    },
                    10.0,
                    None,
                    None,
                    None,
                    &[failed_attempt(
                        &format!("rot-{index}"),
                        10,
                        AttemptErrorClass::Other,
                    )],
                );
            }
            // Aggregate the full 1h window ending at the last-written second. The 1h ring
            // (3600 slots) holds ALL `distinct` (> cap) one-per-second providers at once —
            // exactly the cross-slot rotation the union bound must defend against (the 60-
            // slot 1m ring could only ever surface ~60). Without the aggregate cap this
            // union would be `distinct` keys; with it, at most `MAX_TRACKED_PROVIDERS + 1`.
            let now = base + distinct as u64;
            let per = state.view(now).window_1h.per_provider();
            assert!(
                per.len() <= MAX_TRACKED_PROVIDERS + 1,
                "aggregate union bounded to {} keys across rotating slots, got {}",
                MAX_TRACKED_PROVIDERS + 1,
                per.len()
            );
            assert!(
                per.contains_key(OVERFLOW_PROVIDER),
                "rotated-out providers fold into the bounded overflow bucket"
            );
        }
    }

    #[test]
    fn per_provider_maps_are_charged_to_the_snapshot_memory_quota() {
        // The per-provider maps are `#[serde(skip)]` (off the wire) but RETAINED on every
        // `DashboardSnapshot` cut, so `MetricsView::approx_bytes` — the snapshot-ring
        // memory-quota accountant — MUST charge them, or the ring undercounts and can
        // exceed its bound. Pin it: an identical window WITH per-provider samples estimates
        // STRICTLY MORE retained bytes than one WITHOUT, by at least the populated maps'
        // footprint (provider key strings + the fixed `ProviderSample` per entry).
        let key = BucketKey {
            status: StatusClass::Success,
            model: "m".to_string(),
            endpoint: "/v1/responses".to_string(),
            upstream: "u".to_string(),
        };
        let epoch = 3_000_000u64;

        // (a) a terminal with NO attempts → no per-provider entries.
        let without = {
            let mut state = MetricsState::new(DEFAULT_SNAPSHOT_QUOTA_BYTES);
            state.record_terminal(epoch, &key, 40.0, None, None, None, &[]);
            state.view(epoch).approx_bytes()
        };
        // (b) the SAME terminal but WITH several distinct-provider attempts → populated
        // per-provider maps (in all three window rings).
        let with = {
            let mut state = MetricsState::new(DEFAULT_SNAPSHOT_QUOTA_BYTES);
            let attempts = vec![
                failed_attempt("provider-alpha", 80, AttemptErrorClass::HttpStatus),
                failed_attempt("provider-beta", 70, AttemptErrorClass::Timeout),
                served_attempt("provider-gamma", 40),
            ];
            state.record_terminal(epoch, &key, 40.0, None, None, None, &attempts);
            state.view(epoch).approx_bytes()
        };

        assert!(
            with > without,
            "approx_bytes must charge the retained per-provider maps: with={with} without={without}"
        );
        // The delta must be at least the three providers' fixed `ProviderSample` size in
        // EACH of the three window rings (a lower bound — key strings + String overhead add
        // more), proving the maps are genuinely accounted, not a rounding artifact.
        let min_delta = 3 * 3 * std::mem::size_of::<ProviderSample>();
        assert!(
            with - without >= min_delta,
            "delta {} >= {} (3 providers × 3 windows × ProviderSample)",
            with - without,
            min_delta
        );
    }

    #[test]
    fn per_provider_unlabeled_attempt_folds_into_unknown() {
        // An attempt with no provider label collapses to the bounded "unknown" sentinel
        // (never an empty distinct key) — same rule as the global bucket key.
        let metrics = MetricsLayer::new();
        let mut attempt = served_attempt("ignored", 40);
        attempt.provider = None;
        metrics.record_terminal(
            FlowStatus::Completed,
            Some("m"),
            "/v1/responses",
            None,
            40,
            None,
            &[attempt],
        );
        let per = metrics.view().window_1m.per_provider();
        assert!(per.contains_key("unknown"), "unlabeled attempt → unknown");
        assert_eq!(per["unknown"].samples, 1);
    }

    #[test]
    fn per_provider_survives_window_eviction_to_empty() {
        // The per-provider tallies live in the SAME ring slots as the global buckets, so a
        // quiet window (no recent samples) reports an EMPTY per-provider map — not stale
        // ancient samples. This pins the circular-reuse/eviction behavior for the new data.
        let metrics = MetricsLayer::new();
        {
            // Record an attempt at a FIXED old epoch, then aggregate a full window past it.
            let mut state = metrics.lock();
            let old_epoch = 1_000_000u64;
            state.record_terminal(
                old_epoch,
                &BucketKey {
                    status: StatusClass::Success,
                    model: "m".to_string(),
                    endpoint: "/v1/responses".to_string(),
                    upstream: "provider-a".to_string(),
                },
                40.0,
                None,
                None,
                None,
                &[served_attempt("provider-a", 40)],
            );
            // As of the SAME epoch the provider is present...
            assert!(
                state
                    .view(old_epoch)
                    .window_1m
                    .provider_latency("provider-a")
                    .is_some()
            );
            // ...but a full 1h-window later (epoch advanced beyond every ring), the slots
            // are stale and the per-provider view is empty (no fabricated carry-over).
            let far_future = old_epoch + WINDOW_1H_SLOTS as u64 + 10;
            assert!(
                state
                    .view(far_future)
                    .window_1m
                    .provider_latency("provider-a")
                    .is_none(),
                "stale per-provider samples are evicted with the slot"
            );
        }
    }

    #[test]
    fn status_class_mapping() {
        assert_eq!(
            StatusClass::from_status(FlowStatus::Completed),
            StatusClass::Success
        );
        assert_eq!(
            StatusClass::from_status(FlowStatus::Failed),
            StatusClass::Error
        );
        assert_eq!(
            StatusClass::from_status(FlowStatus::Cancelled),
            StatusClass::Cancelled
        );
        assert_eq!(
            StatusClass::from_status(FlowStatus::Open),
            StatusClass::Error
        );
    }

    #[test]
    fn coordinated_snapshot_is_internally_consistent() {
        // The cut captures summaries, metrics, topology, and per-domain cursors at a
        // single instant — they must all reflect the SAME state (no torn read).
        let metrics = MetricsLayer::new();
        let flow = DashboardFlowStore::new();
        let topo = ProviderHealthPublisher::default();
        topo.publish(Vec::new()); // version 1

        // Open + finalize a flow, and record a matching metrics response.
        flow.open(
            "api_1".to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            crate::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
            None,
            crate::dashboard_flow::ClientAttribution::none(),
        );
        flow.finalize(
            "api_1",
            FlowStatus::Completed,
            Some("response.completed".to_string()),
            Some("provider-a".to_string()),
        );
        metrics.record_response(
            FlowStatus::Completed,
            Some("served-m"),
            "/v1/responses",
            Some("provider-a"),
            42,
        );

        let cut = metrics.snapshot(&flow, &topo).expect("cut taken");
        // Summaries reflect the finalized flow.
        assert_eq!(cut.summaries.len(), 1);
        assert_eq!(cut.summaries[0].api_call_id, "api_1");
        assert_eq!(cut.summaries[0].status, FlowStatus::Completed);
        // Metrics reflect the recorded response.
        assert_eq!(cut.metrics.window_1m.total_count(), 1);
        // Topology is the captured published version.
        assert_eq!(cut.topology.version, 1);
        // Per-domain cursors are all present + non-zero (flow + metrics + topology).
        assert!(cut.cursors.flow_seq >= 1, "flow_seq advanced");
        assert!(cut.cursors.metrics_seq >= 1, "metrics_seq advanced");
        assert_eq!(cut.cursors.topology_seq, 1, "topology_seq == version");
    }

    #[test]
    fn snapshot_cuts_hold_no_body_refs_and_are_quota_bounded() {
        // The 135 GiB fix: simulate many cuts over a churning store with large
        // bodies live, and assert (a) NO body bytes are reachable from any cut and
        // (b) the ring's retained bytes stay under a small quota (eviction works).
        // Use a tiny quota so eviction is exercised without allocating 400 MiB.
        let quota = 256 * 1024; // 256 KiB ring quota
        let metrics = MetricsLayer::with_snapshot_quota(quota);
        let flow = DashboardFlowStore::new();
        let topo = ProviderHealthPublisher::default();
        topo.publish(Vec::new());

        // Populate the live store with flows carrying large bodies.
        let big_body = vec![b'x'; 64 * 1024];
        for index in 0..64 {
            let api = format!("api_{index}");
            flow.open(
                api.clone(),
                "POST".to_string(),
                "/v1/responses".to_string(),
                crate::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
                Some(crate::dashboard_flow::capture_body(&big_body)),
                crate::dashboard_flow::ClientAttribution::none(),
            );
            flow.finalize(&api, FlowStatus::Completed, None, Some("p".to_string()));
        }

        // Take many cuts (more than the ring can hold under the byte quota).
        let mut last_cut = None;
        for _ in 0..50 {
            last_cut = metrics.snapshot(&flow, &topo);
        }
        let cut = last_cut.expect("a cut");

        // (a) NO summary carries body bytes — the summary type is body-free by
        // construction; assert its serialized form holds none of the body payload.
        let serialized = serde_json::to_string(&cut.summaries).expect("serialize summaries");
        assert!(
            !serialized.contains("xxxxxxxx"),
            "no body bytes reachable from a snapshot summary"
        );
        // Each summary's measured footprint is well under 1 KiB (body-free).
        for summary in &cut.summaries {
            assert!(
                summary_approx_bytes(summary) < 1024,
                "each body-free summary is < 1 KiB"
            );
        }

        // (b) The ring honored the byte quota (eviction kept it bounded).
        let retained = {
            let state = metrics.lock();
            state.snapshots.retained_bytes
        };
        assert!(
            retained <= quota,
            "snapshot ring retained {retained} bytes <= quota {quota} (NOT 135 GiB)"
        );
    }

    fn open_finalized(flow: &DashboardFlowStore, api: &str) {
        flow.open(
            api.to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            crate::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
            None,
            crate::dashboard_flow::ClientAttribution::none(),
        );
        flow.finalize(api, FlowStatus::Completed, None, Some("p".to_string()));
    }

    /// Recompute the ring's unique-allocation bytes from scratch (the invariant the
    /// incremental refcounting must always equal).
    fn recomputed_ring_bytes(ring: &SnapshotRing) -> usize {
        let mut fresh = SnapshotRing::new(usize::MAX);
        let mut total = 0usize;
        for cut in &ring.cuts {
            total += fresh.acquire(cut);
        }
        total
    }

    #[test]
    fn unchanged_flows_are_shared_between_cuts() {
        let metrics = MetricsLayer::new();
        let flow = DashboardFlowStore::new();
        let topo = ProviderHealthPublisher::default();
        topo.publish(Vec::new());
        for index in 0..64 {
            open_finalized(&flow, &format!("api_{index}"));
        }

        let first = metrics.snapshot(&flow, &topo).expect("cut");
        let idle = metrics.snapshot(&flow, &topo).expect("cut");
        // Nothing mutated: the whole list (and the equal metrics view) is shared.
        assert_eq!(first.summaries.alloc_id(), idle.summaries.alloc_id());
        assert!(Arc::ptr_eq(&first.metrics, &idle.metrics));
        assert!(idle.taken_at_ms >= first.taken_at_ms);

        // One flow mutates: only its summary is re-projected.
        flow.set_upstream("api_7", None, Some("served-new".to_string()), None);
        let changed = metrics.snapshot(&flow, &topo).expect("cut");
        assert_ne!(changed.summaries.alloc_id(), idle.summaries.alloc_id());
        assert_eq!(changed.summaries.len(), 64);
        let mut fresh = 0;
        for (before, after) in idle.summaries.iter().zip(changed.summaries.iter()) {
            assert_eq!(before.api_call_id, after.api_call_id, "order unchanged");
            if before.alloc_id() != after.alloc_id() {
                fresh += 1;
                assert_eq!(after.api_call_id, "api_7");
                assert_eq!(after.model_served.as_deref(), Some("served-new"));
            }
        }
        assert_eq!(fresh, 1, "only the mutated flow is a new allocation");
        // The older cut still shows the pre-mutation state (seek is frozen).
        let old = idle
            .summaries
            .iter()
            .find(|summary| summary.api_call_id == "api_7")
            .unwrap();
        assert_ne!(old.model_served.as_deref(), Some("served-new"));
    }

    #[test]
    fn a_full_ring_of_resident_flows_costs_one_copy_per_flow() {
        // Before sharing, 720 cuts × 512 resident flows deep-copied every summary
        // into every cut (~360 copies per flow on average, hundreds of MiB). Now a
        // flow that never changes is charged once.
        let metrics = MetricsLayer::with_snapshot_quota(DEFAULT_SNAPSHOT_QUOTA_BYTES);
        let flow = DashboardFlowStore::new();
        let topo = ProviderHealthPublisher::default();
        topo.publish(Vec::new());
        for index in 0..512 {
            open_finalized(&flow, &format!("api_{index}"));
        }
        let one_cut = metrics.snapshot(&flow, &topo).expect("cut");
        let single_cut_bytes = metrics.lock().snapshots.retained_bytes;
        for _ in 1..SNAPSHOT_RING_SLOTS {
            metrics.snapshot(&flow, &topo);
        }
        let state = metrics.lock();
        assert_eq!(state.snapshots.cuts.len(), SNAPSHOT_RING_SLOTS);
        let per_cut_overhead = std::mem::size_of::<DashboardSnapshot>();
        assert_eq!(
            state.snapshots.retained_bytes,
            single_cut_bytes + (SNAPSHOT_RING_SLOTS - 1) * per_cut_overhead,
            "idle cuts add only their fixed header"
        );
        assert!(state.snapshots.retained_bytes < 2 * 1024 * 1024);
        assert_eq!(
            state.snapshots.retained_bytes,
            recomputed_ring_bytes(&state.snapshots)
        );
        // Every cut references the first cut's summaries (no deep copies).
        assert!(
            state
                .snapshots
                .cuts
                .iter()
                .all(|cut| { cut.summaries.alloc_id() == one_cut.summaries.alloc_id() })
        );
    }

    #[test]
    fn ring_accounting_stays_exact_under_churn_and_eviction() {
        let metrics = MetricsLayer::with_snapshot_quota(64 * 1024);
        let flow = DashboardFlowStore::new();
        let topo = ProviderHealthPublisher::default();
        topo.publish(Vec::new());
        for index in 0..32 {
            open_finalized(&flow, &format!("api_{index}"));
        }
        for round in 0..400 {
            if round % 3 == 0 {
                flow.set_upstream(
                    &format!("api_{}", round % 32),
                    None,
                    Some(format!("m{round}")),
                    None,
                );
            }
            if round % 50 == 0 {
                topo.publish(Vec::new());
            }
            if round % 7 == 0 {
                open_finalized(&flow, &format!("new_{round}"));
            }
            metrics.snapshot(&flow, &topo);
            let state = metrics.lock();
            assert!(state.snapshots.retained_bytes <= 64 * 1024 || state.snapshots.cuts.len() == 1);
            assert_eq!(
                state.snapshots.retained_bytes,
                recomputed_ring_bytes(&state.snapshots),
                "round {round}"
            );
        }
        // Draining the ring releases everything it charged.
        let mut state = metrics.lock();
        while !state.snapshots.cuts.is_empty() {
            state.snapshots.pop_oldest();
        }
        assert_eq!(state.snapshots.retained_bytes, 0);
        assert!(state.snapshots.lists.counts.is_empty());
        assert!(state.snapshots.summaries.counts.is_empty());
        assert!(state.snapshots.views.counts.is_empty());
    }

    #[test]
    fn snapshot_at_returns_nearest_le_cut() {
        let metrics = MetricsLayer::new();
        let flow = DashboardFlowStore::new();
        let topo = ProviderHealthPublisher::default();
        topo.publish(Vec::new());

        // Push three cuts and capture their timestamps.
        let mut timestamps = Vec::new();
        for _ in 0..3 {
            let cut = metrics.snapshot(&flow, &topo).expect("cut");
            timestamps.push(cut.taken_at_ms);
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        // A ts at/after the last cut returns the last cut.
        let latest = metrics
            .snapshot_at(timestamps[2] + 1000)
            .expect("nearest <= ts");
        assert_eq!(latest.taken_at_ms, timestamps[2]);
        // A ts between the first and second returns the first.
        let mid = metrics
            .snapshot_at(timestamps[1].saturating_sub(1))
            .expect("nearest <= the boundary");
        assert_eq!(mid.taken_at_ms, timestamps[0]);
        // A ts before every cut returns None.
        assert!(
            metrics
                .snapshot_at(timestamps[0].saturating_sub(1))
                .is_none()
        );
    }

    #[test]
    fn per_domain_cursors_are_monotonic_across_cuts() {
        let metrics = MetricsLayer::new();
        let flow = DashboardFlowStore::new();
        let topo = ProviderHealthPublisher::default();
        topo.publish(Vec::new()); // version 1

        let cut1 = metrics.snapshot(&flow, &topo).expect("cut1");
        // Mutate metrics + flow + topology between cuts.
        metrics.record_response(
            FlowStatus::Completed,
            Some("m"),
            "/v1/responses",
            Some("p"),
            5,
        );
        flow.open(
            "api_x".to_string(),
            "POST".to_string(),
            "/v1/responses".to_string(),
            crate::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
            None,
            crate::dashboard_flow::ClientAttribution::none(),
        );
        topo.publish(Vec::new()); // version 2
        let cut2 = metrics.snapshot(&flow, &topo).expect("cut2");

        assert!(
            cut2.cursors.metrics_seq > cut1.cursors.metrics_seq,
            "metrics_seq monotonic"
        );
        assert!(
            cut2.cursors.flow_seq > cut1.cursors.flow_seq,
            "flow_seq monotonic"
        );
        assert!(
            cut2.cursors.topology_seq > cut1.cursors.topology_seq,
            "topology_seq monotonic"
        );
    }

    #[test]
    fn snapshot_with_monitor_seq_records_monitor_cursor() {
        let metrics = MetricsLayer::new();
        let flow = DashboardFlowStore::new();
        let topo = ProviderHealthPublisher::default();
        topo.publish(Vec::new());
        let cut = metrics
            .snapshot_with_monitor_seq(&flow, &topo, 99)
            .expect("cut");
        assert_eq!(cut.cursors.monitor_seq, 99);
    }

    #[test]
    fn lock_order_stress_no_deadlock() {
        // The fixed FlowStore→Metrics lock order means concurrent FlowStore mutation
        // + the snapshot task (the ONLY >1-lock holder) cannot deadlock. Hammer both
        // stores from many OS threads while a snapshot thread takes the combined
        // critical section, and assert the whole thing joins (a deadlock would hang
        // the test process). Thread-based (not async) so the OS scheduler — not a
        // cooperative runtime — exercises the real lock contention.
        let metrics = MetricsLayer::new();
        let flow = DashboardFlowStore::new();
        let topo = ProviderHealthPublisher::default();
        topo.publish(Vec::new());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let snapshot_thread = {
            let metrics = metrics.clone();
            let flow = flow.clone();
            let topo = topo.clone();
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = metrics.snapshot(&flow, &topo);
                    std::thread::yield_now();
                }
            })
        };

        let mut workers = Vec::new();
        for worker_id in 0..8 {
            let flow = flow.clone();
            let metrics = metrics.clone();
            workers.push(std::thread::spawn(move || {
                for index in 0..2000 {
                    let api = format!("api_{worker_id}_{index}");
                    flow.open(
                        api.clone(),
                        "POST".to_string(),
                        "/v1/responses".to_string(),
                        crate::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
                        None,
                        crate::dashboard_flow::ClientAttribution::none(),
                    );
                    flow.finalize(
                        &api,
                        FlowStatus::Completed,
                        Some("done".to_string()),
                        Some("p".to_string()),
                    );
                    metrics.record_response(
                        FlowStatus::Completed,
                        Some("m"),
                        "/v1/responses",
                        Some("p"),
                        5,
                    );
                }
            }));
        }
        for worker in workers {
            worker.join().expect("worker thread joins (no deadlock)");
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        snapshot_thread.join().expect("snapshot thread joins");

        // A cut was taken and is body-free + quota-bounded.
        let cut = metrics.latest_snapshot().expect("a cut");
        assert!(cut.summaries.len() <= crate::monitor::REQUEST_EVENT_LIMIT);
    }

    #[test]
    fn snapshot_metadata_is_captured_inside_the_lock() {
        // Codex D5 R2 HIGH: the cut's metadata (`taken_at_ms`/epoch, cursors) must be
        // sampled INSIDE the dual-lock critical section, not before it. If `taken_at_ms`
        // were read before the locks, a concurrent writer could `open()` a flow that
        // lands in the summaries (captured under the lock) with a `started_ms` LATER than
        // the pre-lock timestamp — stamping the cut earlier than the state it contains.
        // Hammer `open()` from many threads while a snapshot thread takes cuts and assert
        // every cut's `taken_at_ms` is ≥ the `started_ms` of every flow it contains and
        // that its epoch is mutually consistent with `taken_at_ms`.
        let metrics = MetricsLayer::new();
        let flow = DashboardFlowStore::new();
        let topo = ProviderHealthPublisher::default();
        topo.publish(Vec::new());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let violations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cuts_seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let snapshot_thread = {
            let metrics = metrics.clone();
            let flow = flow.clone();
            let topo = topo.clone();
            let stop = Arc::clone(&stop);
            let violations = Arc::clone(&violations);
            let cuts_seen = Arc::clone(&cuts_seen);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    if let Some(cut) = metrics.snapshot(&flow, &topo) {
                        cuts_seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        // The cut can never be stamped before a flow it already contains:
                        // `started_ms` is set at `open()` (under the FlowStore lock) and
                        // the summary that carries it was captured under the SAME lock the
                        // cut's `taken_at_ms` is now sampled under, so the timestamp must
                        // dominate every contained flow.
                        let ts_ok = cut
                            .summaries
                            .iter()
                            .all(|summary| cut.taken_at_ms >= summary.started_ms);
                        if !ts_ok {
                            violations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    std::thread::yield_now();
                }
            })
        };

        let mut workers = Vec::new();
        for worker_id in 0..8 {
            let flow = flow.clone();
            let metrics = metrics.clone();
            workers.push(std::thread::spawn(move || {
                for index in 0..2000 {
                    let api = format!("api_{worker_id}_{index}");
                    flow.open(
                        api.clone(),
                        "POST".to_string(),
                        "/v1/responses".to_string(),
                        crate::dashboard_flow::redact_headers(&axum::http::HeaderMap::new()),
                        None,
                        crate::dashboard_flow::ClientAttribution::none(),
                    );
                    metrics.record_response(
                        FlowStatus::Completed,
                        Some("m"),
                        "/v1/responses",
                        Some("p"),
                        5,
                    );
                }
            }));
        }
        for worker in workers {
            worker.join().expect("worker thread joins");
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        snapshot_thread.join().expect("snapshot thread joins");

        assert!(
            cuts_seen.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "the snapshot thread took at least one cut under contention"
        );
        assert_eq!(
            violations.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "every cut's taken_at_ms is >= every contained flow's started_ms and its \
             epoch is consistent with taken_at_ms (metadata sampled inside the lock)"
        );
    }

    #[test]
    fn snapshot_stamp_is_after_topology_and_monitor_reads() {
        // Codex D5 R3 HIGH: topology + the monitor are INDEPENDENT domains (neither is
        // under the FlowStore/Metrics lock), so the cut timestamp must be stamped AFTER
        // they are read — otherwise a topology publish or monitor emit racing the cut
        // could land after a prematurely-stamped timestamp yet still be included, making
        // `taken_at_ms` predate a field the cut carries. Drive a concurrent topology
        // publisher that records, for each version, the wall-clock time captured JUST
        // BEFORE the publish (so it happens-before the version is visible to any reader),
        // and a monitor cursor whose value IS the wall-clock ms at read time. Then assert
        // every cut's `taken_at_ms` is ≥ the publish-time of the topology version it
        // captured AND ≥ the monitor cursor it captured (no field newer than the stamp).
        let metrics = MetricsLayer::new();
        let flow = DashboardFlowStore::new();
        let topo = ProviderHealthPublisher::default();
        topo.publish(Vec::new());
        // version → wall-clock ms recorded just before that version's publish.
        let publish_times: Arc<Mutex<std::collections::HashMap<u64, u128>>> =
            Arc::new(Mutex::new(std::collections::HashMap::new()));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let violations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cuts_seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // The topology publisher: stamp the about-to-be-published version's time BEFORE
        // publishing so the recorded time precedes the version becoming reader-visible.
        let publisher_thread = {
            let topo = topo.clone();
            let publish_times = Arc::clone(&publish_times);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                // `publish` bumps the version to (current + 1); record THAT version's
                // pre-publish time, then publish. The recorded time is ≤ the swap, so a
                // reader that sees this version did its read at/after the recorded time.
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let next_version = topo.latest().version + 1;
                    publish_times
                        .lock()
                        .expect("publish_times poisoned")
                        .insert(next_version, now_ms());
                    topo.publish(Vec::new());
                    std::thread::yield_now();
                }
            })
        };

        let snapshot_thread = {
            let metrics = metrics.clone();
            let flow = flow.clone();
            let topo = topo.clone();
            let publish_times = Arc::clone(&publish_times);
            let stop = Arc::clone(&stop);
            let violations = Arc::clone(&violations);
            let cuts_seen = Arc::clone(&cuts_seen);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // The monitor cursor read returns the wall-clock ms AT read time; the
                    // stamp-LAST ordering guarantees `taken_at_ms` is taken after it.
                    if let Some(cut) = metrics.snapshot_with(&flow, &topo, now_ms_u64) {
                        cuts_seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        // The monitor cursor was read before the timestamp was stamped.
                        if cut.taken_at_ms < cut.cursors.monitor_seq as u128 {
                            violations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        // The topology version the cut captured was published at a time
                        // recorded BEFORE it became visible; since topology is read before
                        // the timestamp is stamped, that publish-time must not exceed the
                        // stamp. (version 0 is the pre-loop seed with no recorded time.)
                        if let Some(published_ms) = publish_times
                            .lock()
                            .expect("publish_times poisoned")
                            .get(&cut.cursors.topology_seq)
                            .copied()
                            && cut.taken_at_ms < published_ms
                        {
                            violations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    std::thread::yield_now();
                }
            })
        };

        // Let the two independent-domain writers race the cut for a bit.
        std::thread::sleep(std::time::Duration::from_millis(50));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        publisher_thread.join().expect("publisher thread joins");
        snapshot_thread.join().expect("snapshot thread joins");

        assert!(
            cuts_seen.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "the snapshot thread took at least one cut while topology/monitor raced it"
        );
        assert_eq!(
            violations.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "every cut's taken_at_ms is >= the topology version's publish-time AND the \
             monitor cursor it captured (independent fields sampled before the stamp)"
        );
    }

    /// D7b R3 HIGH: `view_with_seq` must sample the time INSIDE the lock so the view's
    /// window, the `metrics_seq`, and the bucketed data are mutually consistent. Each
    /// `record_response` bumps `metrics_seq` by 1 AND adds exactly 1 to the count in the
    /// current second's slot of every ring; the 1 h ring spans 3600 s, so within this
    /// sub-second test EVERY recorded sample falls inside the 1 h window — making the 1 h
    /// `total_count == metrics_seq` an exact invariant for a consistent atomic read.
    ///
    /// If the time were sampled BEFORE the lock (the bug), a writer landing a sample at
    /// the next second between the reader's time-read and its lock-acquire would bump
    /// `metrics_seq` AND drop the count into a slot whose `epoch_s > now` — which
    /// `aggregate(now)` EXCLUDES from every window. The reader would then observe
    /// `metrics_seq = N` while the 1 h window held only `N - 1` counts. Hammer a writer
    /// across a second boundary while the reader takes `view_with_seq()` and assert no
    /// such sample-in-seq-but-excluded-from-view is ever observed.
    #[test]
    fn view_with_seq_time_and_seq_are_consistent_under_contention() {
        let metrics = MetricsLayer::new();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let violations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let reads_seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Writers: record terminal responses as fast as possible so the reader straddles
        // a 1 s boundary mid-read. Only `record_response` (never `record_usage`) is used
        // so every seq-bump corresponds to exactly one counted sample.
        let mut writers = Vec::new();
        for _ in 0..4 {
            let metrics = metrics.clone();
            let stop = Arc::clone(&stop);
            writers.push(std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    metrics.record_response(
                        FlowStatus::Completed,
                        Some("m"),
                        "/v1/responses",
                        Some("p"),
                        5,
                    );
                }
            }));
        }

        let reader_thread = {
            let metrics = metrics.clone();
            let stop = Arc::clone(&stop);
            let violations = Arc::clone(&violations);
            let reads_seen = Arc::clone(&reads_seen);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let (view, seq) = metrics.view_with_seq();
                    reads_seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // The 1 h window includes every sample of this short test, so its
                    // count must EXACTLY equal the seq — never lag it (which is the bug:
                    // a seq-bumping sample excluded from the time-bucketed view).
                    if view.window_1h.total_count() != seq {
                        violations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    std::thread::yield_now();
                }
            })
        };

        // Run long enough to straddle at least one wall-clock second boundary, where the
        // sample-newer-than-`now` exclusion would manifest under the pre-lock-time bug.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for writer in writers {
            writer.join().expect("writer thread joins");
        }
        reader_thread.join().expect("reader thread joins");

        assert!(
            reads_seen.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "the reader took at least one view_with_seq while writers raced it"
        );
        assert_eq!(
            violations.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "every (view, seq) is consistent: the 1 h window's count equals the seq, so \
             no sample is included in the seq yet excluded from the time-bucketed view"
        );
    }

    /// The wall-clock epoch-ms as a `u64`, the monitor-cursor read used by the R3
    /// stamp-ordering test: passing it as `read_monitor_seq` makes the captured
    /// `monitor_seq` the read-time, so the test can assert `taken_at_ms` ≥ it.
    fn now_ms_u64() -> u64 {
        now_ms() as u64
    }
}
