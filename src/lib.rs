//! # oxide-loadshed
//!
//! Load shedding for GPU job queues with ternary admission control.
//!
//! Admission decisions: `+1` (admit), `0` (queue), `-1` (shed).
//! Configurable thresholds with priority-based graceful degradation,
//! coordinated upstream backoff, and gradual recovery.

use std::collections::VecDeque;
use std::time::Instant;

// ── Admission decision ─────────────────────────────────────────────

/// Ternary admission decision for incoming jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Admit the job immediately — capacity is available.
    Admit = 1,
    /// Queue the job — approaching capacity, but not yet critical.
    Queue = 0,
    /// Shed (reject) the job — over capacity.
    Shed = -1,
}

// ── Job & priority ─────────────────────────────────────────────────

/// Job priority level. Higher values = higher priority (kept longer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Priority(pub u8);

impl Priority {
    pub const LOWEST: Priority = Priority(0);
    pub const LOW: Priority = Priority(64);
    pub const NORMAL: Priority = Priority(128);
    pub const HIGH: Priority = Priority(192);
    pub const CRITICAL: Priority = Priority(255);
}

/// A GPU job awaiting execution.
#[derive(Debug, Clone)]
pub struct Job {
    pub id: u64,
    pub priority: Priority,
    /// Estimated GPU memory in bytes.
    pub size: u64,
    pub submitted_at: Instant,
}

// ── Configuration ──────────────────────────────────────────────────

/// Thresholds for the load shedder.
#[derive(Debug, Clone)]
pub struct LoadShedConfig {
    /// Total capacity (e.g. GPU memory in bytes).
    pub capacity: u64,
    /// Below this ratio → `Admit`. Default 0.70.
    pub admit_threshold: f64,
    /// Between admit and this ratio → `Queue`. Default 0.90.
    pub shed_threshold: f64,
    /// How aggressively to recover admission during drain (0..1).
    pub recovery_rate: f64,
}

impl Default for LoadShedConfig {
    fn default() -> Self {
        Self {
            capacity: 100,
            admit_threshold: 0.70,
            shed_threshold: 0.90,
            recovery_rate: 0.10,
        }
    }
}

impl LoadShedConfig {
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity,
            ..Self::default()
        }
    }

    fn admit_limit(&self) -> u64 {
        (self.capacity as f64 * self.admit_threshold) as u64
    }

    fn shed_limit(&self) -> u64 {
        (self.capacity as f64 * self.shed_threshold) as u64
    }
}

// ── Statistics ─────────────────────────────────────────────────────

/// Runtime statistics for the load shedder.
#[derive(Debug, Clone, Default)]
pub struct ShedStats {
    pub total_admitted: u64,
    pub total_queued: u64,
    pub total_shed: u64,
    pub shed_by_priority: std::collections::HashMap<Priority, u64>,
    /// Recent queue-depth snapshots (timestamp, depth).
    pub queue_depth_history: VecDeque<(Instant, usize)>,
}

impl ShedStats {
    fn record_shed(&mut self, priority: Priority) {
        self.total_shed += 1;
        *self.shed_by_priority.entry(priority).or_insert(0) += 1;
    }
}

// ── Upstream notification ──────────────────────────────────────────

/// Signal sent to upstream producers for coordinated shedding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffSignal {
    /// Everything is fine, produce normally.
    Normal,
    /// Approaching limits, slow down.
    SlowDown,
    /// Critical — stop or drastically reduce production.
    Stop,
}

// ── Job queue ──────────────────────────────────────────────────────

/// Priority-aware job queue with size tracking.
#[derive(Debug)]
pub struct JobQueue {
    jobs: VecDeque<Job>,
    used: u64,
}

impl JobQueue {
    pub fn new() -> Self {
        Self {
            jobs: VecDeque::new(),
            used: 0,
        }
    }

    /// Number of jobs in the queue.
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    /// Total size of all queued jobs.
    pub fn used_capacity(&self) -> u64 {
        self.used
    }

    /// Enqueue a job, maintaining descending priority order (highest first).
    pub fn enqueue(&mut self, job: Job) {
        self.used += job.size;
        // Insert so that highest priority is at the front.
        let pos = self
            .jobs
            .iter()
            .position(|j| job.priority > j.priority)
            .unwrap_or(self.jobs.len());
        self.jobs.insert(pos, job);
    }

    /// Dequeue the highest-priority job.
    pub fn dequeue(&mut self) -> Option<Job> {
        let job = self.jobs.pop_front()?;
        self.used = self.used.saturating_sub(job.size);
        Some(job)
    }

    /// Shed the lowest-priority job (from the back).
    pub fn shed_lowest(&mut self) -> Option<Job> {
        let job = self.jobs.pop_back()?;
        self.used = self.used.saturating_sub(job.size);
        Some(job)
    }

    /// Shed jobs from the bottom until `used` ≤ `target`.
    /// Returns the shed jobs.
    pub fn shed_to(&mut self, target: u64) -> Vec<Job> {
        let mut shed = Vec::new();
        while self.used > target && !self.jobs.is_empty() {
            if let Some(job) = self.shed_lowest() {
                shed.push(job);
            }
        }
        shed
    }
}

impl Default for JobQueue {
    fn default() -> Self {
        Self::new()
    }
}

// ── Load shedder ───────────────────────────────────────────────────

/// The main load-shedding controller.
pub struct LoadShedder {
    config: LoadShedConfig,
    queue: JobQueue,
    stats: ShedStats,
    /// Tracks how many consecutive drain cycles for recovery.
    drain_streak: u32,
}

impl LoadShedder {
    pub fn new(config: LoadShedConfig) -> Self {
        Self {
            config,
            queue: JobQueue::new(),
            stats: ShedStats::default(),
            drain_streak: 0,
        }
    }

    /// Current queue reference.
    pub fn queue(&self) -> &JobQueue {
        &self.queue
    }

    /// Mutable queue reference (for completing / dequeueing jobs).
    pub fn queue_mut(&mut self) -> &mut JobQueue {
        &mut self.queue
    }

    /// Statistics snapshot.
    pub fn stats(&self) -> &ShedStats {
        &self.stats
    }

    /// Evaluate an incoming job and return an admission decision.
    pub fn evaluate(&mut self, job: &Job) -> Admission {
        let projected = self.queue.used_capacity() + job.size;
        let decision = self.decide(projected);
        match decision {
            Admission::Admit => {
                self.stats.total_admitted += 1;
            }
            Admission::Queue => {
                self.stats.total_queued += 1;
            }
            Admission::Shed => {
                self.stats.record_shed(job.priority);
            }
        }
        decision
    }

    /// Actually admit + enqueue a job (call after `evaluate` returns Admit or Queue).
    pub fn admit(&mut self, job: Job) {
        self.queue.enqueue(job);
        self.record_depth();
        self.drain_streak = 0;
    }

    /// Complete (dequeue) the next highest-priority job.
    pub fn complete(&mut self) -> Option<Job> {
        let job = self.queue.dequeue()?;
        self.record_depth();
        // If queue is draining, bump recovery counter.
        let used_ratio = self.queue.used_capacity() as f64 / self.config.capacity as f64;
        if used_ratio < self.config.admit_threshold {
            self.drain_streak += 1;
        }
        Some(job)
    }

    /// Perform coordinated shedding: shed lowest-priority jobs until
    /// usage falls below the admit threshold. Returns shed jobs.
    pub fn coordinated_shed(&mut self) -> Vec<Job> {
        let target = self.config.admit_limit();
        let shed = self.queue.shed_to(target);
        for job in &shed {
            self.stats.record_shed(job.priority);
        }
        self.record_depth();
        shed
    }

    /// Determine the backoff signal to send upstream.
    pub fn backoff_signal(&self) -> BackoffSignal {
        let ratio = self.queue.used_capacity() as f64 / self.config.capacity as f64;
        if ratio >= self.config.shed_threshold {
            BackoffSignal::Stop
        } else if ratio >= self.config.admit_threshold {
            BackoffSignal::SlowDown
        } else {
            BackoffSignal::Normal
        }
    }

    /// Recovery: returns an adjusted admit threshold that gradually
    /// increases as the queue drains over multiple cycles.
    pub fn recovery_admit_threshold(&self) -> f64 {
        let base = self.config.admit_threshold;
        let bump = self.drain_streak as f64 * self.config.recovery_rate * 0.05;
        // Cap at shed_threshold so we never exceed it.
        (base + bump).min(self.config.shed_threshold - 0.01)
    }

    // ── internals ──────────────────────────────────────────────

    fn decide(&self, projected: u64) -> Admission {
        let cap = self.config.capacity;
        // Use recovery-adjusted threshold if we have a drain streak.
        let admit_limit = if self.drain_streak > 0 {
            (cap as f64 * self.recovery_admit_threshold()) as u64
        } else {
            self.config.admit_limit()
        };
        let shed_limit = self.config.shed_limit();

        if projected <= admit_limit {
            Admission::Admit
        } else if projected <= shed_limit {
            Admission::Queue
        } else {
            Admission::Shed
        }
    }

    fn record_depth(&mut self) {
        // Keep last 256 snapshots.
        if self.stats.queue_depth_history.len() >= 256 {
            self.stats.queue_depth_history.pop_front();
        }
        self.stats
            .queue_depth_history
            .push_back((Instant::now(), self.queue.len()));
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_job(id: u64, priority: Priority, size: u64) -> Job {
        Job {
            id,
            priority,
            size,
            submitted_at: Instant::now(),
        }
    }

    #[test]
    fn test_admit_below_threshold() {
        let mut shedder = LoadShedder::new(LoadShedConfig::new(100));
        let job = make_job(1, Priority::NORMAL, 10);
        assert_eq!(shedder.evaluate(&job), Admission::Admit);
    }

    #[test]
    fn test_queue_in_middle_zone() {
        // capacity=100, admit at 70, shed at 90
        let mut shedder = LoadShedder::new(LoadShedConfig::new(100));
        // Fill to 65
        shedder.admit(make_job(1, Priority::NORMAL, 65));
        // 65 + 10 = 75 → Queue zone
        let job = make_job(2, Priority::NORMAL, 10);
        assert_eq!(shedder.evaluate(&job), Admission::Queue);
    }

    #[test]
    fn test_shed_above_threshold() {
        let mut shedder = LoadShedder::new(LoadShedConfig::new(100));
        shedder.admit(make_job(1, Priority::NORMAL, 85));
        // 85 + 10 = 95 → Shed zone
        let job = make_job(2, Priority::NORMAL, 10);
        assert_eq!(shedder.evaluate(&job), Admission::Shed);
    }

    #[test]
    fn test_priority_ordering() {
        let mut q = JobQueue::new();
        q.enqueue(make_job(1, Priority::LOW, 10));
        q.enqueue(make_job(2, Priority::HIGH, 10));
        q.enqueue(make_job(3, Priority::NORMAL, 10));
        // Dequeue should be highest priority first
        assert_eq!(q.dequeue().unwrap().priority, Priority::HIGH);
        assert_eq!(q.dequeue().unwrap().priority, Priority::NORMAL);
        assert_eq!(q.dequeue().unwrap().priority, Priority::LOW);
    }

    #[test]
    fn test_shed_lowest_priority() {
        let mut shedder = LoadShedder::new(LoadShedConfig::new(100));
        shedder.admit(make_job(1, Priority::CRITICAL, 30));
        shedder.admit(make_job(2, Priority::NORMAL, 30));
        shedder.admit(make_job(3, Priority::LOW, 30));
        // 90 used → shed lowest until ≤ 70
        let shed = shedder.coordinated_shed();
        assert!(shed.iter().any(|j| j.priority == Priority::LOW));
        // Only LOW shed: 90 - 30 = 60 ≤ 70
        assert!(shed.iter().all(|j| j.priority != Priority::CRITICAL));
        assert!(shedder.queue().used_capacity() <= 70);
    }

    #[test]
    fn test_backoff_signal() {
        let mut shedder = LoadShedder::new(LoadShedConfig::new(100));
        assert_eq!(shedder.backoff_signal(), BackoffSignal::Normal);
        shedder.admit(make_job(1, Priority::NORMAL, 75));
        assert_eq!(shedder.backoff_signal(), BackoffSignal::SlowDown);
        shedder.admit(make_job(2, Priority::NORMAL, 20));
        assert_eq!(shedder.backoff_signal(), BackoffSignal::Stop);
    }

    #[test]
    fn test_stats_tracking() {
        let mut shedder = LoadShedder::new(LoadShedConfig::new(100));
        shedder.evaluate(&make_job(1, Priority::NORMAL, 10)); // Admit
        shedder.admit(make_job(1, Priority::NORMAL, 10));
        shedder.admit(make_job(2, Priority::NORMAL, 65));
        shedder.evaluate(&make_job(3, Priority::LOW, 5)); // Queue zone: 75+5=80
        shedder.evaluate(&make_job(4, Priority::LOW, 5)); // Queue: 75+5=80
        shedder.evaluate(&make_job(5, Priority::LOW, 20)); // Shed: 75+20=95
        let stats = shedder.stats();
        assert_eq!(stats.total_admitted, 1);
        assert_eq!(stats.total_queued, 2);
        assert_eq!(stats.total_shed, 1);
    }

    #[test]
    fn test_recovery_increases_threshold() {
        let mut shedder = LoadShedder::new(LoadShedConfig::new(100));
        shedder.admit(make_job(1, Priority::NORMAL, 60));
        // Complete jobs to build drain streak
        shedder.complete();
        shedder.complete();
        let recovered = shedder.recovery_admit_threshold();
        assert!(recovered > shedder.config.admit_threshold);
        assert!(recovered < shedder.config.shed_threshold);
    }

    #[test]
    fn test_queue_depth_history() {
        let mut shedder = LoadShedder::new(LoadShedConfig::new(100));
        shedder.admit(make_job(1, Priority::NORMAL, 10));
        shedder.admit(make_job(2, Priority::NORMAL, 10));
        shedder.complete();
        let history = &shedder.stats().queue_depth_history;
        // Should have 3 snapshots (2 admits + 1 complete)
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].1, 1); // after first admit
        assert_eq!(history[1].1, 2); // after second admit
        assert_eq!(history[2].1, 1); // after complete
    }
}
