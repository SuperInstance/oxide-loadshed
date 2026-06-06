# oxide-loadshed

Load shedding for GPU job queues with ternary admission. Priority-based shedding, coordinated backoff, graceful recovery.

## Why This Matters

# oxide-loadshed
Load shedding for GPU job queues with ternary admission control.
Admission decisions: `+1` (admit), `0` (queue), `-1` (shed).
Configurable thresholds with priority-based graceful degradation,
coordinated upstream backoff, and gradual recovery.

## The Five-Layer Stack

This crate is part of the **Oxide Stack** — a distributed GPU runtime built on five layers:

```
┌─────────────────┐
│  cudaclaw        │  Persistent GPU kernels, warp consensus, SmartCRDT
├─────────────────┤
│  cuda-oxide      │  Flux → MIR → Pliron → NVVM → PTX compiler
├─────────────────┤
│  flux-core       │  Bytecode VM + A2A agent protocol
├─────────────────┤
│  pincher         │  "Vector DB as runtime, LLM as compiler"
├─────────────────┤
│  open-parallel   │  Async runtime (tokio fork)
└─────────────────┘
```

The key insight: **ternary values {-1, 0, +1} map directly to GPU compute**. They pack 16× denser than FP32, enable XNOR+popcount matmul, and conservation laws become compile-time checks.

## Design

Every value in this crate follows **ternary algebra** (Z₃):

| Value | Meaning | GPU Analog |
|-------|---------|------------|
| +1 | Positive / Active / Healthy | Warp vote yes |
| 0 | Neutral / Pending / Balanced | Warp vote abstain |
| -1 | Negative / Failed / Overloaded | Warp vote no |

This isn't arbitrary — ternary is the natural encoding for:
1. **BitNet b1.58** (Microsoft) — ternary LLMs at 60% less power
2. **GPU warp voting** — hardware ballot returns ternary consensus
3. **Conservation laws** — {-1, 0, +1} preserves quantity

## Key Types

```rust
pub enum Admission
pub struct Priority
pub struct Job
pub struct LoadShedConfig
pub fn new
pub struct ShedStats
pub enum BackoffSignal
pub struct JobQueue
pub fn new
pub fn len
pub fn is_empty
pub fn used_capacity
```

## Usage

```toml
[dependencies]
oxide-loadshed = "0.1.0"
```

```rust
use oxide_loadshed::*;
// See src/lib.rs tests for complete working examples
```

## Testing

```bash
git clone https://github.com/SuperInstance/oxide-loadshed.git
cd oxide-loadshed
cargo test    # 9 tests
```

## Stats

| Metric | Value |
|--------|-------|
| Tests | 9 |
| Lines of Rust | 458 |
| Public API | 27 items |

## License

Apache-2.0
