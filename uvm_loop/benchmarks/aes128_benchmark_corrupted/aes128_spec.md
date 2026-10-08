# AES-128 Single-Block Benchmark Specification

## 1. Purpose

This benchmark is a self-contained AES-128 encryption RTL design intended for
LLM-driven RTL verification using Cocotb and PyUVM.

The DUT implements AES-128 encryption for one 128-bit plaintext block under
one 128-bit key.

The benchmark is intentionally limited to a single block and ECB-style
encryption. It does not implement padding, message chaining, or a streaming
interface.

## 2. DUT Interface

```systemverilog
module aes128 (
    input  logic         clk,
    input  logic         rst_n,
    input  logic         start,
    input  logic [127:0] key,
    input  logic [127:0] plaintext,
    output logic         done,
    output logic [127:0] ciphertext
);
```

### Inputs

- `clk`: Rising-edge clock.
- `rst_n`: Active-low synchronous reset.
- `start`: Starts a new encryption transaction when asserted while the DUT is
  idle.
- `key`: 128-bit AES-128 encryption key.
- `plaintext`: 128-bit plaintext block.

### Outputs

- `done`: Pulses high for one clock cycle when `ciphertext` is valid.
- `ciphertext`: 128-bit AES-128 encrypted output.

## 3. Transaction Protocol

1. Assert `rst_n = 0` for at least one rising clock edge.
2. Deassert `rst_n`.
3. Drive `key` and `plaintext`.
4. Assert `start = 1` for one rising clock edge.
5. Deassert `start`.
6. Wait for `done = 1`.
7. Sample `ciphertext` when `done` is high.

A new transaction should only be started after the previous transaction has
completed.

The expected implementation latency is 10 encryption rounds after the
initial AddRoundKey operation. Verification should primarily use the `done`
signal rather than relying on a hard-coded cycle count.

## 4. AES-128 Functional Requirements

The DUT shall implement standard AES-128 encryption as defined by FIPS-197.

AES-128 consists of:

- 128-bit plaintext
- 128-bit key
- 10 encryption rounds
- 128-bit ciphertext

The initial transformation is:

`State = Plaintext XOR Key`

Rounds 1 through 9 perform:

1. SubBytes
2. ShiftRows
3. MixColumns
4. AddRoundKey

Round 10 performs:

1. SubBytes
2. ShiftRows
3. AddRoundKey

MixColumns is omitted from the final round.

## 5. State and Byte Ordering

The benchmark uses the conventional AES byte ordering.

For a 128-bit value represented as:

`00 11 22 33 44 55 66 77 88 99 aa bb cc dd ee ff`

the first byte is the most-significant byte of the Verilog vector.

AES state bytes are arranged column-major:

```text
|  0   4   8  12 |
|  1   5   9  13 |
|  2   6  10  14 |
|  3   7  11  15 |
```

This ordering must be preserved when converting between Python integers/bytes
and the Verilog `logic [127:0]` signals.

## 6. Reference Model

`aes128_reference_model.py` provides an independent executable Python model.

It implements:

- AES S-box
- key expansion
- SubBytes
- ShiftRows
- MixColumns
- AddRoundKey
- 10-round AES-128 encryption

The reference model is intentionally separate from the RTL implementation
structure so that the verification environment has an independent golden
model.

## 7. Required Verification Properties

A generated Cocotb/PyUVM environment should verify at least:

### Functional correctness

For each transaction:

`DUT.ciphertext == reference_model.aes128_encrypt(plaintext, key)`

### Control behavior

- Reset clears the internal transaction state.
- `start` initiates encryption only while idle.
- `done` is asserted when the output becomes valid.
- `done` should not remain permanently high.
- The ciphertext must correspond to the key and plaintext associated with
  the transaction.

### Key sensitivity

Changing the key while keeping plaintext fixed should generally change the
ciphertext.

### Plaintext sensitivity

Changing plaintext while keeping the key fixed should generally change the
ciphertext.

### Determinism

Repeated encryption of the same plaintext with the same key must produce the
same ciphertext.

## 8. Mandatory Known-Answer Tests

### FIPS-197 official vector

Key:

`000102030405060708090a0b0c0d0e0f`

Plaintext:

`00112233445566778899aabbccddeeff`

Expected ciphertext:

`69c4e0d86a7b0430d8cdb78070b4c55a`

### Zero vector

Key:

`00000000000000000000000000000000`

Plaintext:

`00000000000000000000000000000000`

Expected ciphertext:

`66e94bd4ef8a2c3b884cfa59ca342b2e`

### Incrementing vector

Key:

`000102030405060708090a0b0c0d0e0f`

Plaintext:

`000102030405060708090a0b0c0d0e0f`

Expected ciphertext:

`0a940bb5416ef045f1c39458c653ea5a`

## 9. Benchmark Scope

Included:

- AES-128 encryption
- Single 128-bit block
- 128-bit encryption key
- Clocked transaction interface
- `start` / `done` handshake

Not included:

- AES-192
- AES-256
- Decryption
- CBC/CTR/GCM modes
- Padding
- Multi-block messages
- Key expansion interface
- DMA or AXI interfaces

## 10. Suggested ARGUS Verification Challenges

This benchmark provides multiple opportunities for RTL verification and
repair, including:

- S-box lookup errors
- ShiftRows indexing errors
- MixColumns arithmetic errors
- Key expansion/Rcon errors
- Round-count errors
- Final-round MixColumns mistakes
- AddRoundKey mistakes
- State byte-ordering errors
- Reset/control errors
- `start`/`done` protocol errors

These faults can be introduced independently or cumulatively to evaluate
the ability of an LLM-driven verification and RTL-repair loop to detect,
localize, and repair functional RTL defects.
