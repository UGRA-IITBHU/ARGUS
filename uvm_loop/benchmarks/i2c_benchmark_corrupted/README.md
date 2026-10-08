# I2C Master ARGUS Benchmark

Self-contained I2C master benchmark for LLM-driven Cocotb + PyUVM RTL
verification.

## Files

```text
i2c_benchmark/
├── i2c_master.sv
├── i2c_reference_model.py
├── i2c_spec.md
└── README.md
```

## Supported transactions

- 7-bit slave addressing
- One-byte write
- One-byte read
- ACK/NACK handling
- START/STOP sequencing
- Busy/done transaction control
- ACK error reporting

## Reference-model test

```bash
python i2c_reference_model.py
```

The Python model checks address-byte construction and transaction-level
expectations. Cocotb/PyUVM should provide the actual bus driving/monitoring
and compare the DUT behavior against these protocol expectations.

## ARGUS usage

Use `i2c_spec.md` as the hardware specification and
`i2c_reference_model.py` as the independent reference model for generating
and validating the Cocotb + PyUVM verification environment.
