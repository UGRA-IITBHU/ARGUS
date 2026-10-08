# Hamming Encoder Benchmark

Extended Hamming(72,64) SECDED encoder for ARGUS/LLM-driven RTL verification.

Files:

- `hamming_encoder.sv` — synthesizable combinational RTL.
- `hamming_reference_model.py` — independent Python golden model.
- `hamming_spec.md` — detailed functional specification.

Architecture:

```text
64-bit data
    |
    v
Hamming parity insertion
    |
    +-- P1 P2 P4 P8 P16 P32 P64
    |
    v
71-bit Hamming code
    |
    +-- overall parity
    |
    v
72-bit SECDED codeword
```

Run the reference model:

```bash
python3 hamming_reference_model.py
```

The encoder uses even parity and places parity bits at Hamming positions
1, 2, 4, 8, 16, 32 and 64.
