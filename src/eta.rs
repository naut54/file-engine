use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crate::progress::Progress;

/// Minimum wall time a rate sample must cover before it's folded into the
/// running average. Completion events arrive in bursts (a whole batch's
/// entries finish near-simultaneously), so a rate computed over the
/// microseconds between two of them is noise, not throughput.
const SAMPLE_WINDOW: Duration = Duration::from_millis(500);

/// Weight given to each new sample. Low enough that a single slow batch
/// doesn't make the estimate lurch, high enough to track a genuine
/// slowdown (a USB write cache filling up, say) within a few seconds.
const EWMA_ALPHA: f64 = 0.3;

/// Which cost regime the currently-executing work belongs to. The three
/// have genuinely different cost drivers, which is the whole reason this
/// type exists — see `EtaEstimator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Regime {
    Directory,
    SmallFile,
    LargeFile,
}

/// An observed rate of "work units per second" for one regime, where a
/// work unit is a directory, a file, or a byte depending on the regime.
///
/// Time and work are accumulated separately rather than as (work, elapsed)
/// pairs: work arrives on completion events, elapsed time accrues
/// continuously, and the two only need to line up at flush boundaries.
#[derive(Debug, Clone, Default)]
struct Rate {
    ewma: Option<f64>,
    pending_work: f64,
    pending_secs: f64,
}

impl Rate {
    fn add_work(&mut self, work: f64) {
        self.pending_work += work;
    }

    fn add_time(&mut self, secs: f64) {
        self.pending_secs += secs;
        if self.pending_secs >= SAMPLE_WINDOW.as_secs_f64() {
            let sample = self.pending_work / self.pending_secs;
            self.ewma = Some(match self.ewma {
                Some(previous) => previous * (1.0 - EWMA_ALPHA) + sample * EWMA_ALPHA,
                None => sample,
            });
            self.pending_work = 0.0;
            self.pending_secs = 0.0;
        }
    }

    /// `None` until there's something to divide — a caller with
    /// outstanding work in this regime and no rate yet genuinely cannot
    /// estimate, and should say so rather than guess.
    ///
    /// Falls back to the un-flushed partial sample so a run that finishes
    /// in under one `SAMPLE_WINDOW` still reports something.
    fn per_sec(&self) -> Option<f64> {
        if let Some(ewma) = self.ewma {
            if ewma > 0.0 {
                return Some(ewma);
            }
        }
        if self.pending_secs > 0.0 && self.pending_work > 0.0 {
            return Some(self.pending_work / self.pending_secs);
        }
        None
    }
}

/// Predicts how much longer an operation has left, from the `Progress`
/// events it emits.
///
/// # Why not just bytes-done over elapsed
///
/// A single bytes-per-second figure is wrong for this crate's pipeline in
/// three specific ways, and this type exists to correct each:
///
/// 1. **Two cost regimes.** Small files are packed into batches and are
///    syscall-bound — their cost is essentially per-file and barely
///    depends on size. Large files are streamed and are bandwidth-bound.
///    A bytes/sec rate learned during the small-file phase overestimates
///    the large-file phase badly, and vice versa, so the two are measured
///    separately and recombined.
/// 2. **The directory pre-pass isn't in `bytes_total`.** It runs before
///    `Progress::Started` is ever emitted and can dominate a run on a slow
///    filesystem (a real exFAT-over-USB copy spent about a minute creating
///    ~7,700 directories). It gets its own per-directory cost term.
/// 3. **Default batch sort is `SortOrder::Descending`.** The largest
///    entries complete first, so the mix observed early in a run is not
///    representative of what's left — extrapolating remaining work from
///    observed work converges on the wrong answer. `Progress::Planned`
///    supplies the true split up front instead.
///
/// # How wall time is attributed
///
/// A second of wall time is charged to *every* regime that had work in
/// flight during it, not to a single "current" regime. Small and large
/// files genuinely do run at the same time: the dispatcher enqueues every
/// batch before any stream, but a workload small enough to fit inside the
/// concurrency limit starts all of them at once, and then a streaming
/// large file overlaps the entire small-file phase. Charging that second
/// to only one of them leaves the other with work recorded but no elapsed
/// time to divide it by — an infinite rate, or more precisely no usable
/// rate at all.
///
/// The regimes are then recombined the way the pipeline actually runs
/// them: the directory pre-pass finishes strictly before dispatch begins,
/// so its cost adds, while small and large files overlap, so theirs is a
/// maximum rather than a sum.
///
/// ```text
/// estimate = directories + max(small files, large files)
/// ```
///
/// # Where the numbers come from, in order of authority
///
/// 1. **`EntryProgress` samples** — bytes observed landing at the
///    destination while a large file is still in flight. The most direct
///    measurement available, and the only one that exists during a single
///    long transfer.
/// 2. **Completed large files** — an exact byte count over an exact
///    duration, folded in the same way.
/// 3. **Overall byte throughput** — used for outstanding large bytes
///    before either of the above has produced anything. Dominated by
///    batched small files, which pay per-file overhead that streaming
///    doesn't, so it reads low and the estimate starts pessimistic.
///
/// Bytes credited by (1) are not re-counted by (2); a completing entry
/// contributes only what sampling hadn't already seen.
///
/// A copy the filesystem satisfies by copy-on-write (APFS `clonefile`,
/// reflinks) finishes before the first sample and produces no rate at all
/// — correctly, since there is nothing to wait for. Measured here at 2GB
/// in under a millisecond.
///
/// # Usage
///
/// ```no_run
/// # async fn example(engine: &file_engine::FileEngine) -> file_engine::Result<()> {
/// use file_engine::EtaEstimator;
/// use tokio_stream::StreamExt;
///
/// let mut handle = engine.copy("src", "dst").start()?;
/// let mut eta = EtaEstimator::new();
///
/// while let Some(progress) = handle.progress().next().await {
///     eta.observe(&progress);
///     if let Some(remaining) = eta.estimate() {
///         println!("{}s remaining", remaining.as_secs());
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// Purely observational: it performs no I/O, spawns nothing, and holds no
/// reference to the running operation. Feeding it events out of order, or
/// only some of them, degrades the estimate but never panics.
#[derive(Debug, Clone)]
pub struct EtaEstimator {
    small_file_threshold: u64,
    directories_remaining: usize,
    small_files_remaining: usize,
    large_bytes_remaining: u64,
    directory_rate: Rate,
    small_file_rate: Rate,
    large_file_rate: Rate,
    /// Bytes per second across every completed entry regardless of regime.
    /// Used only to stand in for `large_file_rate` before any large file
    /// has finished — see `estimate`.
    overall_byte_rate: Rate,
    /// Entries currently between `EntryStarted` and their terminal event,
    /// per regime — the basis for deciding which regimes a span of wall
    /// time is charged to. Counts, not booleans, because several entries
    /// of the same regime are normally in flight at once.
    small_in_flight: usize,
    large_in_flight: usize,
    /// The directory pre-pass reports no per-directory start event, so it
    /// counts as in flight from `DirectoriesStarted` until the last
    /// directory is accounted for.
    directories_in_flight: bool,
    /// Bytes already counted for entries still in flight, from
    /// `EntryProgress` samples. Keyed by source path, and cleared when the
    /// entry reaches a terminal event, so this holds at most one key per
    /// concurrently streaming file.
    credited_bytes: HashMap<PathBuf, u64>,
    last_event: Option<Instant>,
    /// Set by `Planned`, cleared by the `Started` that follows it. Lets a
    /// `Started` arriving *without* a preceding `Planned` be recognised as
    /// a metadata-only phase (the delete sweeps) and modelled as per-entry
    /// cost, rather than being mistaken for a phase whose plan went
    /// missing.
    awaiting_planned_start: bool,
}

impl Default for EtaEstimator {
    fn default() -> Self {
        Self::new()
    }
}

impl EtaEstimator {
    pub fn new() -> Self {
        Self {
            small_file_threshold: 0,
            directories_remaining: 0,
            small_files_remaining: 0,
            large_bytes_remaining: 0,
            directory_rate: Rate::default(),
            small_file_rate: Rate::default(),
            large_file_rate: Rate::default(),
            overall_byte_rate: Rate::default(),
            small_in_flight: 0,
            large_in_flight: 0,
            directories_in_flight: false,
            credited_bytes: HashMap::new(),
            last_event: None,
            awaiting_planned_start: false,
        }
    }

    /// Feeds one event in. Call this for every event on the stream: each
    /// one either supplies work done or marks the boundary of a span of
    /// wall time, and skipping events costs accuracy in both.
    pub fn observe(&mut self, progress: &Progress) {
        self.observe_at(progress, Instant::now());
    }

    fn observe_at(&mut self, progress: &Progress, now: Instant) {
        // What was in flight over the interval that just ended — captured
        // before the match, which may start or retire work.
        let was_in_flight = self.in_flight_regimes();

        match progress {
            Progress::Planned {
                directories,
                small_files,
                large_bytes,
                small_file_threshold,
                ..
            } => {
                self.small_file_threshold = *small_file_threshold;
                self.directories_remaining = *directories;
                self.small_files_remaining = *small_files;
                self.large_bytes_remaining = *large_bytes;
                self.awaiting_planned_start = true;
            }

            Progress::DirectoriesStarted { total } => {
                self.directories_remaining = *total;
                self.directories_in_flight = *total > 0;
            }

            Progress::DirectoryCompleted { .. } | Progress::DirectoryFailed { .. } => {
                self.directory_rate.add_work(1.0);
                self.directories_remaining = self.directories_remaining.saturating_sub(1);
                if self.directories_remaining == 0 {
                    self.directories_in_flight = false;
                }
            }

            // A `Started` with no `Planned` before it is a metadata-only
            // phase (delete sweep): no bytes to model, so every entry is
            // charged as one per-operation unit, which is exactly the
            // small-file regime's cost shape.
            Progress::Started { entries_total, .. } => {
                // `dispatch()` emits this only after the directory
                // pre-pass has returned, so it is the definitive end of
                // that phase — without this, an unfinished-looking
                // directory count keeps absorbing the file phase's wall
                // time and drags the per-directory rate toward zero.
                self.directories_in_flight = false;

                if self.awaiting_planned_start {
                    self.awaiting_planned_start = false;
                } else {
                    self.small_files_remaining = *entries_total;
                    self.large_bytes_remaining = 0;
                    self.small_file_threshold = u64::MAX;
                }
            }

            Progress::EntryStarted { entry } => {
                if self.regime_for(entry.size) == Regime::LargeFile {
                    self.large_in_flight += 1;
                } else {
                    self.small_in_flight += 1;
                }
            }

            Progress::EntryCompleted { entry } | Progress::EntryFailed { entry } => {
                // A failure still consumed wall time and still retired an
                // entry, so it counts toward the rate exactly as a success
                // does — otherwise a run failing every entry would report
                // a rate of zero and never produce an estimate at all.
                if self.regime_for(entry.size) == Regime::LargeFile {
                    // Only the bytes not already credited by in-flight
                    // sampling — otherwise a sampled file is counted twice
                    // and reports double its real throughput.
                    let outstanding = entry
                        .size
                        .saturating_sub(self.credited_bytes.remove(&entry.path).unwrap_or(0));
                    self.large_file_rate.add_work(outstanding as f64);
                    self.overall_byte_rate.add_work(outstanding as f64);
                    self.large_bytes_remaining =
                        self.large_bytes_remaining.saturating_sub(outstanding);
                    self.large_in_flight = self.large_in_flight.saturating_sub(1);
                } else {
                    self.overall_byte_rate.add_work(entry.size as f64);
                    self.small_file_rate.add_work(1.0);
                    self.small_files_remaining = self.small_files_remaining.saturating_sub(1);
                    self.small_in_flight = self.small_in_flight.saturating_sub(1);
                }
            }

            // Partial progress for an entry still in flight. `bytes_copied`
            // is cumulative, so only the increment since the last sample is
            // new work.
            Progress::EntryProgress {
                entry,
                bytes_copied,
            } => {
                let credited = self.credited_bytes.entry(entry.path.clone()).or_insert(0);
                let delta = bytes_copied.saturating_sub(*credited);
                if delta > 0 {
                    *credited = *bytes_copied;
                    self.large_file_rate.add_work(delta as f64);
                    self.overall_byte_rate.add_work(delta as f64);
                    self.large_bytes_remaining = self.large_bytes_remaining.saturating_sub(delta);
                    // Bytes that have landed are no longer outstanding
                    // work for the in-flight entry either — without this,
                    // the in-flight pool exceeds what actually remains and
                    // the overlapping term is inflated by everything
                    // already copied.
                }
            }
        }

        // Charged *after* the match, so that work reported by this event
        // lands in the same sample window as the interval during which it
        // was performed. Doing it first instead leaves every flushed
        // sample short by exactly the work of the event that triggered it,
        // which reads as a systematic underestimate of throughput — and so
        // a systematic overestimate of time remaining.
        if let Some(last) = self.last_event {
            let elapsed = now.saturating_duration_since(last).as_secs_f64();
            for regime in &was_in_flight {
                self.rate_mut(*regime).add_time(elapsed);
            }
            // Any entry in flight is moving bytes, whichever regime it
            // belongs to.
            if was_in_flight
                .iter()
                .any(|r| matches!(r, Regime::SmallFile | Regime::LargeFile))
            {
                self.overall_byte_rate.add_time(elapsed);
            }
        }
        self.last_event = Some(now);
    }

    /// Estimated time remaining, or `None` while any regime with
    /// outstanding work has no measured rate yet — an operation that has
    /// only just started genuinely has no basis for an estimate, and
    /// reporting nothing is more useful than reporting a fabricated
    /// number that collapses by an order of magnitude a second later.
    ///
    /// Returns `Duration::ZERO` once no work is outstanding.
    pub fn estimate(&self) -> Option<Duration> {
        let seconds_for = |remaining: f64, rate: &Rate| -> Option<f64> {
            if remaining <= 0.0 {
                return Some(0.0);
            }
            Some(remaining / rate.per_sec()?)
        };

        let directories = seconds_for(self.directories_remaining as f64, &self.directory_rate)?;
        let small = seconds_for(self.small_files_remaining as f64, &self.small_file_rate)?;

        // A streamed file reports nothing between `EntryStarted` and
        // `EntryCompleted`, so its own byte rate stays unmeasurable for as
        // long as it takes to copy — on a multi-gigabyte file that is the
        // entire run, i.e. precisely when an ETA is most wanted. Fall back
        // to the byte rate observed across all completed entries. Small
        // files carry per-file overhead that streaming doesn't, so that
        // figure understates streaming throughput: the estimate starts
        // pessimistic and tightens once a large file actually lands, which
        // is the right direction for a countdown to move.
        let large_rate = self
            .large_file_rate
            .per_sec()
            .or_else(|| self.overall_byte_rate.per_sec());
        let seconds_for_bytes = |bytes: u64| -> Option<f64> {
            match (bytes, large_rate) {
                (0, _) => Some(0.0),
                (bytes, Some(rate)) => Some(bytes as f64 / rate),
                (_, None) => None,
            }
        };

        // All outstanding large bytes are treated as overlapping the
        // small-file phase, even though the dispatcher only runs one stream
        // ahead of the batches and queues the rest behind them.
        //
        // The structurally-honest alternative — adding the queued portion
        // rather than maxing it — was implemented and measured, and the
        // result was inconclusive: run-to-run variance on the mixed fixture
        // (6.1s to 8.2s wall time for identical work) is larger than the
        // difference between the two models. `max` is kept as the simpler
        // of the two, not as a demonstrated winner.
        //
        // Note what the additive model would depend on: queued bytes divide
        // by a rate learned while the calibration stream competes with
        // thousands of small files, which understates how fast those
        // streams run once the batches drain and they have the device to
        // themselves. `max` under-counts queued work; the contended rate
        // over-counts its duration. Distinguishing them needs a
        // lower-variance benchmark than this one.
        //
        // The cost of keeping `max` is a visible one: while every remaining
        // stream sits queued, its byte count can't fall and its rate can't
        // update, so the estimate pins at a constant — measured as a
        // countdown frozen at exactly 5.9s for over a second. That is a
        // stale term, not device slowdown; see `dev-docs/design/eta.md` §9.
        let large = seconds_for_bytes(self.large_bytes_remaining)?;

        // Directories add: the pre-pass completes before dispatch starts.
        // Small and large overlap, so the longer of the two absorbs the
        // shorter rather than queueing behind it.
        Duration::try_from_secs_f64(directories + small.max(large)).ok()
    }

    /// Observed throughput for large, streamed files, in bytes per second.
    /// `None` until at least one has completed. Deliberately excludes the
    /// batched small-file phase, whose cost is per-file rather than
    /// per-byte — averaging the two together produces a number that
    /// describes neither.
    pub fn bytes_per_sec(&self) -> Option<f64> {
        self.large_file_rate.per_sec()
    }

    /// Every regime with work in flight right now. A span of wall time is
    /// charged to all of them, since they were all making progress during
    /// it — see the type-level note on attribution.
    fn in_flight_regimes(&self) -> Vec<Regime> {
        let mut regimes = Vec::with_capacity(3);
        if self.directories_in_flight {
            regimes.push(Regime::Directory);
        }
        if self.small_in_flight > 0 {
            regimes.push(Regime::SmallFile);
        }
        if self.large_in_flight > 0 {
            regimes.push(Regime::LargeFile);
        }
        regimes
    }

    fn regime_for(&self, size: u64) -> Regime {
        if size <= self.small_file_threshold {
            Regime::SmallFile
        } else {
            Regime::LargeFile
        }
    }

    fn rate_mut(&mut self, regime: Regime) -> &mut Rate {
        match regime {
            Regime::Directory => &mut self.directory_rate,
            Regime::SmallFile => &mut self.small_file_rate,
            Regime::LargeFile => &mut self.large_file_rate,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::profiler::Entry;

    use super::*;

    fn entry(size: u64) -> Entry {
        Entry {
            path: PathBuf::from("x"),
            relative_path: PathBuf::from("x"),
            size,
            modified: None,
        }
    }

    fn planned(directories: usize, small_files: usize, large_bytes: u64) -> Progress {
        Progress::Planned {
            directories,
            small_files,
            small_bytes: small_files as u64,
            large_files: usize::from(large_bytes > 0),
            large_bytes,
            small_file_threshold: 1024,
        }
    }

    fn started() -> Progress {
        Progress::Started {
            bytes_total: Some(0),
            entries_total: 0,
        }
    }

    /// Drives a sequence of `(event, seconds_since_previous)` through the
    /// estimator against a synthetic clock, so tests assert on the model's
    /// arithmetic rather than on how fast the machine running them is.
    ///
    /// The clock is owned by the caller so that a test replaying two
    /// scripts against one estimator advances a single timeline. Starting
    /// a fresh `Instant::now()` per call instead would place the second
    /// script *behind* the first on the real clock, and every elapsed span
    /// in it would saturate to zero.
    fn replay(estimator: &mut EtaEstimator, clock: &mut Instant, script: &[(Progress, f64)]) {
        for (event, delay) in script {
            *clock += Duration::from_secs_f64(*delay);
            estimator.observe_at(event, *clock);
        }
    }

    #[test]
    fn no_estimate_before_anything_completes() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();
        replay(
            &mut eta,
            &mut clock,
            &[(planned(0, 10, 0), 0.0), (started(), 0.0)],
        );

        assert_eq!(eta.estimate(), None);
    }

    #[test]
    fn estimates_zero_when_nothing_is_outstanding() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();
        replay(
            &mut eta,
            &mut clock,
            &[(planned(0, 0, 0), 0.0), (started(), 0.0)],
        );

        assert_eq!(eta.estimate(), Some(Duration::ZERO));
    }

    #[test]
    fn small_files_are_estimated_per_file_not_per_byte() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();
        let mut script = vec![(planned(0, 100, 0), 0.0), (started(), 0.0)];

        // 10 files over 1s total => 10 files/sec, so the 90 left ~= 9s.
        // Sizes vary 100x across them; a per-byte model would not land on
        // 9s, which is the point of the assertion.
        for i in 0..10 {
            let size = if i % 2 == 0 { 10 } else { 1000 };
            script.push((Progress::EntryStarted { entry: entry(size) }, 0.0));
            script.push((Progress::EntryCompleted { entry: entry(size) }, 0.1));
        }
        replay(&mut eta, &mut clock, &script);

        let estimate = eta.estimate().unwrap().as_secs_f64();
        assert!(
            (estimate - 9.0).abs() < 0.5,
            "expected ~9s for 90 files at 10 files/sec, got {estimate}"
        );
    }

    #[test]
    fn large_files_are_estimated_per_byte() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();
        let big = 10_000_u64;
        let mut script = vec![(planned(0, 0, big * 10), 0.0), (started(), 0.0)];

        // 4 files x 10_000 bytes over 4s => 10_000 bytes/sec, leaving
        // 60_000 bytes => ~6s.
        for _ in 0..4 {
            script.push((Progress::EntryStarted { entry: entry(big) }, 0.0));
            script.push((Progress::EntryCompleted { entry: entry(big) }, 1.0));
        }
        replay(&mut eta, &mut clock, &script);

        let estimate = eta.estimate().unwrap().as_secs_f64();
        assert!(
            (estimate - 6.0).abs() < 0.5,
            "expected ~6s for 60_000 bytes at 10_000 B/s, got {estimate}"
        );
    }

    #[test]
    fn directory_pre_pass_is_estimated_before_any_file_work_is_known() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();
        let mut script = vec![
            (planned(100, 0, 0), 0.0),
            (Progress::DirectoriesStarted { total: 100 }, 0.0),
        ];

        // 20 directories over 2s => 10 dirs/sec, 80 left => ~8s. This is
        // the window that emits no `Started` at all, so an estimator keyed
        // only on `Started`/`bytes_total` would report nothing here.
        for _ in 0..20 {
            script.push((
                Progress::DirectoryCompleted {
                    path: PathBuf::from("d"),
                },
                0.1,
            ));
        }
        replay(&mut eta, &mut clock, &script);

        let estimate = eta.estimate().unwrap().as_secs_f64();
        assert!(
            (estimate - 8.0).abs() < 0.5,
            "expected ~8s for 80 directories at 10/sec, got {estimate}"
        );
    }

    #[test]
    fn directory_and_file_costs_are_summed_not_conflated() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();
        let mut script = vec![
            (planned(30, 30, 0), 0.0),
            (Progress::DirectoriesStarted { total: 30 }, 0.0),
        ];
        // 10 dirs in 1s => 10/sec, 20 dirs left => 2s.
        for _ in 0..10 {
            script.push((
                Progress::DirectoryCompleted {
                    path: PathBuf::from("d"),
                },
                0.1,
            ));
        }
        script.push((started(), 0.0));
        // 10 files in 2s => 5/sec, 20 files left => 4s. Total ~6s.
        for _ in 0..10 {
            script.push((Progress::EntryStarted { entry: entry(10) }, 0.0));
            script.push((Progress::EntryCompleted { entry: entry(10) }, 0.2));
        }
        replay(&mut eta, &mut clock, &script);

        let estimate = eta.estimate().unwrap().as_secs_f64();
        assert!(
            (estimate - 6.0).abs() < 0.7,
            "expected ~6s (2s of directories + 4s of files), got {estimate}"
        );
    }

    #[test]
    fn failed_entries_count_as_progress() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();
        let mut script = vec![(planned(0, 20, 0), 0.0), (started(), 0.0)];
        for _ in 0..10 {
            script.push((Progress::EntryStarted { entry: entry(10) }, 0.0));
            script.push((Progress::EntryFailed { entry: entry(10) }, 0.1));
        }
        replay(&mut eta, &mut clock, &script);

        // A failure consumes wall time and retires an entry just as a
        // success does — 10 done at 10/sec leaves 10 => ~1s.
        let estimate = eta.estimate().unwrap().as_secs_f64();
        assert!((estimate - 1.0).abs() < 0.3, "expected ~1s, got {estimate}");
    }

    #[test]
    fn started_without_planned_is_modelled_as_a_metadata_only_phase() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();
        let mut script = vec![(
            Progress::Started {
                bytes_total: None,
                entries_total: 100,
            },
            0.0,
        )];
        // Deletes carry a real `entry.size` but cost nothing per byte; the
        // phase must be costed per-operation. 10 in 1s => 90 left => ~9s.
        for _ in 0..10 {
            script.push((
                Progress::EntryStarted {
                    entry: entry(5_000_000),
                },
                0.0,
            ));
            script.push((
                Progress::EntryCompleted {
                    entry: entry(5_000_000),
                },
                0.1,
            ));
        }
        replay(&mut eta, &mut clock, &script);

        let estimate = eta.estimate().unwrap().as_secs_f64();
        assert!(
            (estimate - 9.0).abs() < 0.5,
            "expected ~9s for 90 deletions at 10/sec, got {estimate}"
        );
    }

    #[test]
    fn a_later_phase_resets_remaining_work_without_discarding_learned_rates() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();
        let mut script = vec![(planned(0, 10, 0), 0.0), (started(), 0.0)];
        for _ in 0..10 {
            script.push((Progress::EntryStarted { entry: entry(10) }, 0.0));
            script.push((Progress::EntryCompleted { entry: entry(10) }, 0.1));
        }
        replay(&mut eta, &mut clock, &script);
        assert_eq!(eta.estimate(), Some(Duration::ZERO));

        // sync's delete phase: a fresh `Started` announces 5 more entries.
        // The 10 files/sec learned above still applies, so an estimate is
        // available immediately rather than starting from `None` again.
        replay(
            &mut eta,
            &mut clock,
            &[(
                Progress::Started {
                    bytes_total: None,
                    entries_total: 5,
                },
                0.0,
            )],
        );

        let estimate = eta.estimate().unwrap().as_secs_f64();
        assert!(
            (estimate - 0.5).abs() < 0.2,
            "expected ~0.5s for 5 entries at 10/sec, got {estimate}"
        );
    }

    #[test]
    fn slowdown_is_tracked_rather_than_averaged_away() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();
        let mut script = vec![(planned(0, 1000, 0), 0.0), (started(), 0.0)];
        for _ in 0..100 {
            script.push((Progress::EntryStarted { entry: entry(10) }, 0.0));
            script.push((Progress::EntryCompleted { entry: entry(10) }, 0.01));
        }
        replay(&mut eta, &mut clock, &script);
        let fast = eta.estimate().unwrap();

        // Same events, ten times slower. The EWMA must move most of the
        // way toward the new rate; a cumulative average would barely budge.
        let mut script = Vec::new();
        for _ in 0..100 {
            script.push((Progress::EntryStarted { entry: entry(10) }, 0.0));
            script.push((Progress::EntryCompleted { entry: entry(10) }, 0.1));
        }
        replay(&mut eta, &mut clock, &script);
        let slow = eta.estimate().unwrap();

        assert!(
            slow > fast * 3,
            "estimate should track the slowdown: {fast:?} -> {slow:?}"
        );
    }

    /// A streamed large file signals nothing until it completes, so its own
    /// byte rate is unmeasurable while it runs — which on a large enough
    /// file is the whole operation. The estimate must still appear, and
    /// must still account for the outstanding bytes, by falling back to the
    /// byte rate observed across everything that *has* completed.
    ///
    /// Reproduces the shape of a real 2.7GB run that reported no ETA at all
    /// until 95% of the way through, when the first large file landed.
    #[test]
    fn outstanding_large_files_are_costed_from_overall_byte_throughput() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();

        // 100 small files of 100KB, and one 100MB file that never finishes.
        let plan = Progress::Planned {
            directories: 0,
            small_files: 100,
            small_bytes: 10_000_000,
            large_files: 1,
            large_bytes: 100_000_000,
            small_file_threshold: 1_000_000,
        };
        let mut script = vec![
            (plan, 0.0),
            (started(), 0.0),
            (
                Progress::EntryStarted {
                    entry: entry(100_000_000),
                },
                0.0,
            ),
        ];
        // 50 small files x 100KB over 5s => 1MB/s observed overall.
        for _ in 0..50 {
            script.push((
                Progress::EntryStarted {
                    entry: entry(100_000),
                },
                0.0,
            ));
            script.push((
                Progress::EntryCompleted {
                    entry: entry(100_000),
                },
                0.1,
            ));
        }
        replay(&mut eta, &mut clock, &script);

        // 100MB left at ~1MB/s => ~100s, which must dominate the ~5s of
        // remaining small files rather than being dropped from the total.
        let estimate = eta
            .estimate()
            .expect("an unfinished large file must not suppress the estimate")
            .as_secs_f64();
        assert!(
            (estimate - 100.0).abs() < 15.0,
            "expected ~100s dominated by the outstanding large file, got {estimate}"
        );
    }

    /// The fallback above is a stand-in only. A real measurement of large
    /// file throughput must take over as soon as one is available, since
    /// streaming avoids the per-file overhead that the small-file phase
    /// pays and is normally faster per byte.
    #[test]
    fn a_measured_large_file_rate_supersedes_the_overall_fallback() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();

        let plan = Progress::Planned {
            directories: 0,
            small_files: 0,
            small_bytes: 0,
            large_files: 3,
            large_bytes: 300_000_000,
            small_file_threshold: 1_000_000,
        };
        replay(&mut eta, &mut clock, &[(plan, 0.0), (started(), 0.0)]);

        // One 100MB file in 1s => 100MB/s measured directly.
        replay(
            &mut eta,
            &mut clock,
            &[
                (
                    Progress::EntryStarted {
                        entry: entry(100_000_000),
                    },
                    0.0,
                ),
                (
                    Progress::EntryCompleted {
                        entry: entry(100_000_000),
                    },
                    1.0,
                ),
            ],
        );

        assert_eq!(eta.bytes_per_sec(), Some(100_000_000.0));
        let estimate = eta.estimate().unwrap().as_secs_f64();
        assert!(
            (estimate - 2.0).abs() < 0.3,
            "expected ~2s for the remaining 200MB at 100MB/s, got {estimate}"
        );
    }

    /// The case that motivated in-flight sampling: one large file, nothing
    /// else. There is no completion to learn from until the very end, so
    /// without `EntryProgress` this reports nothing for the whole transfer.
    #[test]
    fn a_single_large_file_is_estimated_from_in_flight_samples() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();

        let plan = Progress::Planned {
            directories: 0,
            small_files: 0,
            small_bytes: 0,
            large_files: 1,
            large_bytes: 1_000_000_000,
            small_file_threshold: 1_000_000,
        };
        let big = entry(1_000_000_000);
        let mut script = vec![
            (plan, 0.0),
            (started(), 0.0),
            (Progress::EntryStarted { entry: big.clone() }, 0.0),
        ];
        // 100MB/s: four 0.25s samples, 25MB each.
        for i in 1..=4 {
            script.push((
                Progress::EntryProgress {
                    entry: big.clone(),
                    bytes_copied: i * 25_000_000,
                },
                0.25,
            ));
        }
        replay(&mut eta, &mut clock, &script);

        // 900MB left at 100MB/s => ~9s, while the file is still in flight.
        let estimate = eta
            .estimate()
            .expect("in-flight samples must produce an estimate")
            .as_secs_f64();
        assert!(
            (estimate - 9.0).abs() < 1.0,
            "expected ~9s for the outstanding 900MB at 100MB/s, got {estimate}"
        );
        assert_eq!(eta.bytes_per_sec(), Some(100_000_000.0));
    }

    /// `bytes_copied` is cumulative, and the terminal event carries the
    /// entry's full size. Counting both in full would report a file as
    /// having moved roughly twice its own bytes.
    #[test]
    fn sampled_bytes_are_not_counted_again_on_completion() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();

        let plan = Progress::Planned {
            directories: 0,
            small_files: 0,
            small_bytes: 0,
            large_files: 2,
            large_bytes: 200_000_000,
            small_file_threshold: 1_000_000,
        };
        let big = entry(100_000_000);
        replay(
            &mut eta,
            &mut clock,
            &[
                (plan, 0.0),
                (started(), 0.0),
                (Progress::EntryStarted { entry: big.clone() }, 0.0),
                // 100MB over 1s, reported as four cumulative samples.
                (
                    Progress::EntryProgress {
                        entry: big.clone(),
                        bytes_copied: 25_000_000,
                    },
                    0.25,
                ),
                (
                    Progress::EntryProgress {
                        entry: big.clone(),
                        bytes_copied: 50_000_000,
                    },
                    0.25,
                ),
                (
                    Progress::EntryProgress {
                        entry: big.clone(),
                        bytes_copied: 75_000_000,
                    },
                    0.25,
                ),
                (
                    Progress::EntryProgress {
                        entry: big.clone(),
                        bytes_copied: 100_000_000,
                    },
                    0.25,
                ),
                (Progress::EntryCompleted { entry: big }, 0.0),
            ],
        );

        // 100MB in 1s is 100MB/s. Double-counting would report ~200MB/s
        // and halve the estimate for the remaining file.
        assert_eq!(eta.bytes_per_sec(), Some(100_000_000.0));
        let estimate = eta.estimate().unwrap().as_secs_f64();
        assert!(
            (estimate - 1.0).abs() < 0.2,
            "expected ~1s for the remaining 100MB at 100MB/s, got {estimate}"
        );
    }

    #[test]
    fn out_of_order_and_surplus_completions_do_not_panic() {
        let mut eta = EtaEstimator::new();
        let mut clock = Instant::now();
        replay(
            &mut eta,
            &mut clock,
            &[
                (Progress::EntryCompleted { entry: entry(10) }, 0.1),
                (
                    Progress::DirectoryCompleted {
                        path: PathBuf::from("d"),
                    },
                    0.1,
                ),
                (planned(0, 1, 0), 0.0),
                (started(), 0.0),
                (Progress::EntryCompleted { entry: entry(10) }, 0.1),
                (Progress::EntryCompleted { entry: entry(10) }, 0.1),
            ],
        );

        assert_eq!(eta.estimate(), Some(Duration::ZERO));
    }
}
