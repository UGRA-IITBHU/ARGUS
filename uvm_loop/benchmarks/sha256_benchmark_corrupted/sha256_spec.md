# SHA-256 Single-Block RTL Benchmark Specification

## 1. Purpose

This benchmark evaluates an RTL-verification pipeline on a non-trivial cryptographic datapath.

The design implements the SHA-256 compression algorithm for exactly one
512-bit, already-padded message block and produces the corresponding
256-bit SHA-256 digest.

The benchmark intentionally keeps the hardware interface simple so that
verification effort is focused on functional correctness of the SHA-256
algorithm rather than bus protocols or external memories.

## 2. DUT Interface

Module name:

    sha256

Ports:

    input  logic         clk
    input  logic         rst_n
    input  logic         start
    input  logic [511:0] block
    output logic         done
    output logic [255:0] digest

All multi-byte SHA-256 words are represented in big-endian order.

For the input block:

    block[511:480] = first 32-bit SHA-256 message word
    block[479:448] = second 32-bit word
    ...
    block[31:0]    = sixteenth 32-bit word

## 3. Functional Behavior

The DUT shall implement the SHA-256 compression function specified by
FIPS 180-4.

The implementation shall contain:

1. The eight SHA-256 initial hash values.
2. The 64 SHA-256 round constants K[0..63].
3. A 64-word message schedule W[0..63].
4. The SHA-256 Ch and Maj functions.
5. The large sigma functions:
   - Σ0(x) = ROTR2(x) XOR ROTR13(x) XOR ROTR22(x)
   - Σ1(x) = ROTR6(x) XOR ROTR11(x) XOR ROTR25(x)
6. The small sigma functions:
   - σ0(x) = ROTR7(x) XOR ROTR18(x) XOR SHR3(x)
   - σ1(x) = ROTR17(x) XOR ROTR19(x) XOR SHR10(x)
7. All 64 compression rounds.
8. Feed-forward addition into the eight hash-state words.
9. A 256-bit final digest.

## 4. Input Contract

The `block` input shall contain one complete SHA-256 padded block.

The block must be exactly 512 bits (64 bytes).

The benchmark supports messages whose unpadded length is less than 56 bytes,
because such messages can be represented by one SHA-256 padded block.

For a message M:

    M || 0x80 || 0x00...00 || 64-bit big-endian bit length

must produce exactly 64 bytes.

Examples include:

    ""
    "a"
    "abc"
    "hello world"
    "The quick brown fox jumps over the lazy dog"

The RTL does not perform message padding itself.

## 5. Control Protocol

### Reset

`rst_n` is active-low asynchronous reset.

When `rst_n = 0`:

- `done` shall be 0.
- `digest` shall be 0.
- Internal state shall be reset.
- The DUT shall be ready to accept a new operation.

### Start

A transaction begins when:

    start == 1 && DUT is idle

At this point the DUT shall capture the 512-bit `block`.

`start` may be asserted for one clock cycle.

The input block must remain valid during the start cycle.

### Processing

After accepting `start`, the DUT processes the block.

The reference implementation performs one SHA-256 round per clock cycle.

The DUT is not required to expose intermediate round state.

### Done

When the digest has been computed:

    done == 1

for one clock cycle.

At the same completion event, `digest` shall contain the correct
256-bit SHA-256 digest.

The digest shall be held until reset or until another transaction updates it.

## 6. Output Format

The digest is the concatenation of the eight final 32-bit hash words:

    digest = H0 || H1 || H2 || H3 || H4 || H5 || H6 || H7

with each word represented most-significant bit first.

For the message "abc":

    ba7816bf8f01cfea414140de5dae2223
    b00361a396177a9cb410ff61f20015ad
    ...

Combined as a 256-bit value:

    ba7816bf8f01cfea414140de5dae2223
    b00361a396177a9cb410ff61f20015ad

## 7. Required Verification Properties

A verification environment should check at least:

### Correct digest

For every legal padded input block:

    DUT.digest == SHA256(reference_message)

### Completion

After an accepted start, `done` shall eventually assert.

### One-cycle completion indication

`done` should be a pulse associated with completion of the transaction.

### Reset

Reset shall clear `done` and `digest` and return the DUT to the idle state.

### Transaction independence

A new transaction shall produce a digest determined only by its input
block and shall not depend on the previous transaction.

### Known-answer tests

The following messages should be included:

1. Empty message
2. "a"
3. "abc"
4. "hello world"
5. "The quick brown fox jumps over the lazy dog"

Additional randomized messages shorter than 56 bytes should be generated
by the verification environment.

## 8. Reference Model

`sha256_reference_model.py` is the golden reference.

It uses Python's standard `hashlib.sha256` implementation and provides:

    pad_one_block(message)
    sha256_digest(message)
    sha256_hex(message)
    block_to_digest(block)

The reference model is intentionally independent of the RTL implementation.

## 9. Benchmark Scope

This benchmark is a complete SHA-256 compression implementation for one
512-bit padded block.

It is NOT a streaming multi-block SHA-256 engine.

It does NOT:

- accept arbitrary-length messages directly,
- perform message padding internally,
- expose an AXI interface,
- use external memory,
- implement HMAC,
- implement SHA-224.

These exclusions keep the benchmark focused on the cryptographic
compression datapath.

## 10. Suggested ARGUS Verification Focus

The verification pipeline should be capable of detecting errors in:

- SHA-256 constants
- initial hash values
- message-word ordering
- message schedule expansion
- rotate/shift operations
- Ch function
- Maj function
- Sigma functions
- round-state updates
- feed-forward additions
- control sequencing
- start handling
- done generation
- digest assembly

The verification environment should combine directed known-answer tests,
randomized legal message blocks, reset tests, and transaction sequencing
tests.

## 11. Expected Example

For:

    message = "abc"

the padded block is:

    6162638000000000000000000000000000000000000000000000000000000000
    0000000000000000000000000000000000000000000000000000000000000018

and the expected SHA-256 digest is:

    ba7816bf8f01cfea414140de5dae2223
    b00361a396177a9cb410ff61f20015ad

For:

    message = ""

the padded block is:

    8000000000000000000000000000000000000000000000000000000000000000
    0000000000000000000000000000000000000000000000000000000000000000

and the expected digest is:

    e3b0c44298fc1c149afbf4c8996fb924
    27ae41e4649b934ca495991b7852b855

## 12. Source Reference

The algorithmic definition is based on NIST FIPS 180-4,
Secure Hash Standard (SHS), SHA-256.
