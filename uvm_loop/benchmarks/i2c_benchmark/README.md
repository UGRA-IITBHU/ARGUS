# I2C Benchmark

Self-contained benchmark for the CocoTb + Python UVM / ARGUS verification
pipeline.

## Contents

- `rtl/i2c_master.sv` — candidate SystemVerilog RTL
- `reference_model/i2c_reference_model.py` — independent Python reference model
- `spec/i2c_spec.md` — functional specification

## Intended use

The benchmark can be supplied to the verification-generation pipeline as:

```text
specification + reference model + candidate RTL
```

The Python model is the source of expected transaction behavior. The RTL is
the implementation under test and should not be used to derive expected
results.

## DUT role

The RTL implements a small I2C-master transaction abstraction with:

- 7-bit slave addressing
- supported slave `0x50`
- one-byte register address
- one-byte read/write
- `busy` / `done`
- NACK/error indication
- open-drain-style `SCL` and `SDA`

The benchmark is deliberately compact so that the generated cocotb/pyUVM
environment can exercise both normal operation and incorrect RTL behavior.
