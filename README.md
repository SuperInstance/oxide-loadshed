# oxide-loadshed

Load shedding for GPU job queues with ternary admission control and coordinated backoff.

## Why This Exists

When GPU capacity runs out, you have three choices: accept the job and risk OOM/crash (bad), silently drop it (worse), or explicitly tell the submitter why it was rejected (correct). Load shedding is the disciplined version of that third option. Every incoming job gets a ternary admission decision: **Admit** (+1, capacity available), **Queue** (0, approaching limits but not critical), or **Shed** (-1, over capacity, reject).

The system goes further than simple rejection. It sends **backoff signals** to upstream producers (Normal / SlowDown / Stop), sheds lowest-priority jobs first when forced, and gradually **recovers** admission thresholds as the queue drains. This means the system degrades gracefully under load instead of falling off a cliff.

## Architecture

```
┌───────────────────────────────────────────────────┐
│                 LoadShedder                        │
│  config: LoadShedConfig                           │
│    capacity: 100 GiB                              │
│    admit_threshold: 0.70                          │
│    shed_threshold: 0.90                           │
│                                                   │
│  ┌─────────────────────────────────────┐          │
│  │         JobQueue (priority-sorted)  │          │
│  │                                     │          │
│  │  [CRITICAL] job-5  30 GiB   ← front│          │
│  │  [HIGH]     job-2  20 GiB          │          │
│  │  [NORMAL]   job-1  15 GiB          │          │
│  │  [LOW]      job-3  10 GiB   ← back │          │
│  │                                     │          │
│  │  used: 75 GiB / 100 GiB            │          │
│  └─────────────────────────────────────┘          │
│                                                   │
│  Admission Control:                               │
│  ┌───────────────────────────────────┐            │
│  │ projected ≤ 70 GiB → Admit (+1)  │            │
│  │ 70 < projected ≤ 90 → Queue (0)  │            │
│  │ projected > 90      → Shed (-1)  │            │
│  └───────────────────────────────────┘            │
│                                                   │
│  Backoff Signal to upstream:                      │
│  ratio < 0.70 → Normal (produce freely)           │
│  0.70 ≤ ratio < 0.90 → SlowDown                  │
│  ratio ≥ 0.90 → Stop                             │
│                                                   │
│  Recovery: drain_streak × recovery_rate →         │
│    gradually increases admit threshold             │
└───────────────────────────────────────────────────┘
```

**Key types:**

- `Admission` — `Admit(+1)`, `Queue(0)`, `Shed(-1)` — ternary admission decision
- `Priority` — `LOWEST(0)` to `CRITICAL(255)` — job priority level
- `Job` — id, priority, size (GPU memory), submission timestamp
- `LoadShedConfig` — capacity, admit/shed thresholds, recovery rate
- `JobQueue` — priority-sorted queue with size tracking
- `LoadShedder` — the admission controller
- `BackoffSignal` — `Normal`, `SlowDown`, `Stop` — upstream flow control

## Usage

```rust
use oxide_loadshed::*;

let mut shedder = LoadShedder::new(LoadShedConfig::new(100)); // 100 GiB capacity

// Evaluate incoming jobs
let job = Job { id: 1, priority: Priority::NORMAL, size: 10, submitted_at: Instant::now() };
match shedder.evaluate(&job) {
    Admission::Admit => { shedder.admit(job); }
    Admission::Queue => { shedder.admit(job); } // queued but accepted
    Admission::Shed => { /* reject, notify submitter */ }
}

// Process jobs (highest priority first)
while let Some(job) = shedder.complete() {
    // execute job on GPU
}

// Check upstream backoff signal
match shedder.backoff_signal() {
    BackoffSignal::Normal => { /* submit freely */ }
    BackoffSignal::SlowDown => { /* reduce submission rate */ }
    BackoffSignal::Stop => { /* stop submitting immediately */ }
}

// Emergency: coordinated shed of low-priority jobs
shedder.admit(Job { id: 1, priority: Priority::CRITICAL, size: 30, submitted_at: Instant::now() });
shedder.admit(Job { id: 2, priority: Priority::NORMAL, size: 30, submitted_at: Instant::now() });
shedder.admit(Job { id: 3, priority: Priority::LOW, size: 30, submitted_at: Instant::now() });
let shed = shedder.coordinated_shed(); // sheds LOW first until under admit threshold

// Stats
let stats = shedder.stats();
println!("Admitted: {}, Queued: {}, Shed: {}", 
    stats.total_admitted, stats.total_queued, stats.total_shed);
println!("Hit rate: {:.1}%", shedder.stats().total_admitted as f64 / 
    (stats.total_admitted + stats.total_queued + stats.total_shed) as f64 * 100.0);
```

## API Reference

### `Admission`

```rust
pub enum Admission {
    Admit = 1,   // Capacity available
    Queue = 0,   // Approaching limits
    Shed = -1,   // Over capacity, reject
}
```

### `Priority`

```rust
pub struct Priority(pub u8);
// Constants: LOWEST(0), LOW(64), NORMAL(128), HIGH(192), CRITICAL(255)
```

### `Job`

```rust
pub struct Job { pub id: u64, pub priority: Priority, pub size: u64, pub submitted_at: Instant }
```

### `LoadShedConfig`

- `new(capacity: u64) -> Self` — with default thresholds (0.70 admit, 0.90 shed)
- `capacity: u64`, `admit_threshold: f64`, `shed_threshold: f64`, `recovery_rate: f64`

### `JobQueue`

- `new() -> Self` / `len() -> usize` / `is_empty() -> bool`
- `used_capacity() -> u64` — total size of queued jobs
- `enqueue(job)` — insert in priority order (highest first)
- `dequeue() -> Option<Job>` — remove highest priority
- `shed_lowest() -> Option<Job>` — remove lowest priority
- `shed_to(target: u64) -> Vec<Job>` — shed from bottom until under target

### `LoadShedder`

- `new(config: LoadShedConfig) -> Self`
- `evaluate(job: &Job) -> Admission` — ternary admission decision
- `admit(job: Job)` — enqueue accepted job
- `complete() -> Option<Job>` — dequeue and process highest priority
- `coordinated_shed() -> Vec<Job>` — emergency shed to admit threshold
- `backoff_signal() -> BackoffSignal` — upstream flow control signal
- `recovery_admit_threshold() -> f64` — adjusted threshold during drain
- `queue() -> &JobQueue` / `queue_mut() -> &mut JobQueue`
- `stats() -> &ShedStats`

### `BackoffSignal`

```rust
pub enum BackoffSignal { Normal, SlowDown, Stop }
```

### `ShedStats`

- `total_admitted: u64`, `total_queued: u64`, `total_shed: u64`
- `shed_by_priority: HashMap<Priority, u64>` — per-priority shed counts
- `queue_depth_history: VecDeque<(Instant, usize)>` — last 256 depth snapshots

## The Deeper Idea

This is the **overload protection layer** in the oxide stack's resource architecture. The ternary admission signal (Admit/Queue/Shed) drives real-time decisions, while the backoff signal (Normal/SlowDown/Stop) provides feedback to upstream producers. Together, they implement end-to-end flow control: the shedder doesn't just protect itself, it tells producers how to behave.

The recovery mechanism is the subtle part. After the queue drains below the admit threshold for several consecutive cycles, the admit threshold gradually increases. This prevents the system from oscillating between "shed everything" and "accept everything" — instead, it smoothly transitions back to normal operation. The `recovery_rate` parameter controls how fast this happens: 0.10 means the threshold increases by ~0.5% per drain cycle, so full recovery from a deep shed takes ~20 cycles.

## Related Crates

- **oxide-capacity** — capacity planning that informs shedder thresholds
- **oxide-health-monitor** — GPU health events that trigger more aggressive shedding
- **oxide-tenancy** — multi-tenant isolation that determines per-tenant shed rates
- **oxide-federation** — cross-cluster routing that can offload shed jobs to other clusters
