# Extended Hamming(72,64) SECDED Encoder Specification

## 1. Purpose

This benchmark implements an extended Hamming encoder suitable for RTL
verification research.

The design accepts 64 data bits and generates a 72-bit SECDED codeword:

- 64 data bits
- 7 Hamming parity bits
- 1 overall parity bit

The Hamming parity positions are powers of two:
1, 2, 4, 8, 16, 32, and 64.

This follows the standard Hamming construction in which parity positions are
powers of two and each parity bit covers positions whose binary index contains
the corresponding parity bit. citeturn0search12turn0search6

## 2. Module Interface

Module:

    hamming_encoder

Ports:

    input  logic [63:0] data_in
    output logic [71:0] codeword

The encoder is purely combinational.

No clock, reset, handshake, or external memory is required.

## 3. Codeword Layout

The 72-bit output uses the following convention:

    codeword[0]  = overall parity bit
    codeword[1]  = Hamming position 1
    codeword[2]  = Hamming position 2
    ...
    codeword[71] = Hamming position 71

Hamming positions 1, 2, 4, 8, 16, 32, and 64 are parity positions.

All other Hamming positions contain data bits.

Therefore:

    position 1  -> P1
    position 2  -> P2
    position 3  -> D0
    position 4  -> P4
    position 5  -> D1
    position 6  -> D2
    position 7  -> D3
    position 8  -> P8
    ...
    position 64 -> P64
    ...
    position 71 -> final data bit

The 64 input bits are inserted sequentially from data_in[0] through
data_in[63] into the non-parity Hamming positions.

## 4. Parity Convention

Even parity is required.

For each parity position P:

    P = XOR of all covered data/Hamming bits

including the parity position itself after it has been assigned.

Equivalently, after encoding, every parity check must have XOR result 0.

The coverage rule is:

    position & parity_position != 0

For example:

P1 covers:

    1, 3, 5, 7, 9, 11, ...

P2 covers:

    2, 3, 6, 7, 10, 11, ...

P4 covers:

    4, 5, 6, 7, 12, 13, 14, 15, ...

P8 covers:

    8 through 15, 24 through 31, ...

and similarly for P16, P32 and P64. citeturn0search12

## 5. Overall Parity

An additional parity bit is included to provide the extended Hamming/SECDED
property.

The overall parity bit is:

    codeword[0] = XOR(codeword[1:71])

Therefore, the complete 72-bit codeword has even parity.

The additional overall parity bit allows a decoder to distinguish a
single-bit error from a double-bit error when combined with the Hamming
syndrome. citeturn0search0turn0search5

## 6. Functional Requirements

For every 64-bit input:

1. Every non-power-of-two Hamming position shall contain the corresponding
   input data bit.
2. Positions 1, 2, 4, 8, 16, 32 and 64 shall contain the calculated parity.
3. Every Hamming parity check shall evaluate to zero.
4. The complete 72-bit codeword shall have even parity.
5. The encoder shall be combinational.
6. A change in data_in shall eventually propagate to the corresponding
   codeword output without requiring a clock.

## 7. Reference Model

`hamming_reference_model.py` provides the golden model.

Main function:

    encode(data)

It returns the complete 72-bit SECDED codeword as an integer.

The model also contains known-answer and invariant tests.

Run:

    python3 hamming_reference_model.py

The final line should be:

    All Hamming encoder reference-model tests passed.

## 8. Verification Requirements

A verification environment should test:

### Basic patterns

- all zeros
- all ones
- alternating 1010...
- alternating 0101...

### Structured values

- 0x0000000000000001
- 0x0123456789abcdef
- 0xdeadbeefcafebabe
- 0xffffffffffffffff

### Random testing

Generate a large number of random 64-bit values and compare the DUT output
against the Python reference model.

### Parity invariants

For every generated codeword:

- P1 check must be even
- P2 check must be even
- P4 check must be even
- P8 check must be even
- P16 check must be even
- P32 check must be even
- P64 check must be even
- overall parity must be even

## 9. Suggested ARGUS Fault Injection Targets

This benchmark has many clean semantic fault targets:

- data-to-position mapping
- parity-position identification
- P1 coverage
- P2 coverage
- P4 coverage
- P8 coverage
- P16 coverage
- P32 coverage
- P64 coverage
- even/odd parity inversion
- overall parity
- codeword bit ordering
- data bit ordering
- loop boundaries
- missing parity positions
- wrong output width
- wrong parity calculation
- combinational sensitivity/logic errors

These provide useful fault-injection points for an LLM-driven RTL
verification experiment.

## 10. Scope

This benchmark is an encoder only.

It does not implement:

- error injection
- syndrome calculation
- error correction
- error detection logic
- a decoder
- a clocked interface

The generated 72-bit word is nevertheless an extended Hamming SECDED
codeword and can be consumed by a separate decoder benchmark.
