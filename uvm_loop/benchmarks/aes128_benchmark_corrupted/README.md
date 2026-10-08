# AES-128 ARGUS Benchmark

Self-contained AES-128 single-block encryption benchmark for the ARGUS
LLM-driven Cocotb + PyUVM RTL verification pipeline.

## Files

- `aes128.sv` — AES-128 RTL DUT
- `aes128_reference_model.py` — independent Python golden model
- `aes128_spec.md` — hardware/verification specification

## Interface

```text
clk         : clock
rst_n       : active-low synchronous reset
start       : start encryption transaction
key         : 128-bit AES key
plaintext   : 128-bit plaintext
done        : one-cycle completion pulse
ciphertext  : 128-bit encrypted result
```

## Quick reference-model test

```bash
python aes128_reference_model.py
```

Expected output:

```text
fips_197        69c4e0d86a7b0430d8cdb78070b4c55a
all_zero        66e94bd4ef8a2c3b884cfa59ca342b2e
incrementing    0a940bb5416ef045f1c39458c653ea5a
All AES-128 reference-model tests passed.
```

## ARGUS usage

The specification and reference model can be supplied to ARGUS to generate
the Cocotb + PyUVM verification environment. The RTL can then be verified
against the Python golden model.

The benchmark deliberately uses a compact single-module interface so it can
also be used for controlled RTL fault-injection and repair experiments.
