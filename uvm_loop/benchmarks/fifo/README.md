# FIFO Benchmark

Second benchmark for the generalized ARGUS Cocotb + pyUVM verification flow.

Files:
- `benchmarks/fifo/fifo.sv` — synchronous FIFO RTL
- `benchmarks/fifo/spec.md` — behavioral specification
- `benchmarks/fifo/ref_model.py` — Python reference model
- `benchmarks/fifo/design.yaml` — design configuration

Run from the project root:

```bash
python -m pipeline.run14 --design-config benchmarks/fifo/design.yaml
```

The generated environment should appear under:

```text
generated/designs/fifo/

Within that directory, the generated TB is at `generated/designs/fifo/tb/`,
and all plans, simulator results, checkpoints, and LLM workspaces remain
under the same benchmark directory.
```
