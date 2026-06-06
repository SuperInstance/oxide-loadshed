# oxide-loadshed

Load shedding for GPU job queues with ternary admission. Priority-based shedding, coordinated backoff, graceful recovery.

## Stats

- **Tests**: 9
- **LOC**: 457
- **License**: MIT

## Part of the Oxide Stack

This crate is part of the [Flux→PTX](https://github.com/SuperInstance/cuda-oxide/blob/main/FLUX_TO_PTX.md) experimental suite — a distributed GPU runtime built on five layers:

1. **open-parallel** — async runtime (tokio fork)
2. **pincher** — "Vector DB as runtime, LLM as compiler"
3. **flux-core** — bytecode VM + A2A agent protocol
4. **cuda-oxide** — Flux→MIR→Pliron→NVVM→PTX compiler
5. **cudaclaw** — persistent GPU kernels, warp-level consensus, SmartCRDT

## Usage

```rust
use oxide_loadshed::*;
// See tests in src/lib.rs for complete examples
```

## License

MIT
