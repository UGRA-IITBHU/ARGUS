# SHA-256 ARGUS Benchmark

Self-contained single-block SHA-256 RTL benchmark for LLM-driven RTL verification.

Files:
- `sha256.sv` — synthesizable SHA-256 RTL DUT.
- `sha256_reference_model.py` — independent Python golden model.
- `sha256_spec.md` — functional specification and verification contract.

The DUT accepts one already-padded 512-bit SHA-256 block and produces a
256-bit digest. It does not perform message padding or multi-block processing.

Quick reference-model check:

```bash
python3 sha256_reference_model.py
```

Expected output ends with:

`All SHA-256 reference-model tests passed.`
