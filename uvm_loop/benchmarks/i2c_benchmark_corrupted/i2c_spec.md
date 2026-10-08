# I2C Master Benchmark Specification

## 1. Purpose

This benchmark is a compact single-byte I2C master transaction engine for
LLM-driven RTL verification using Cocotb and PyUVM.

The DUT supports:

- 7-bit I2C slave addresses
- Write transactions
- One-byte read transactions
- Slave ACK detection
- Master NACK after a one-byte read
- START and STOP transaction states
- Busy and done status
- Open-drain-style SDA/SCL output controls

The benchmark deliberately focuses on the protocol/control logic rather than
implementing a complete multi-byte I2C controller.

## 2. DUT Interface

```systemverilog
module i2c_master #(
    parameter int CLK_DIV = 4
) (
    input  logic       clk,
    input  logic       rst_n,

    input  logic       start,
    input  logic [6:0] slave_addr,
    input  logic       rw,
    input  logic [7:0] tx_data,

    input  logic       sda_in,
    input  logic       scl_in,

    output logic [7:0] rx_data,
    output logic       done,
    output logic       busy,
    output logic       ack_error,

    output logic       sda_out,
    output logic       sda_oe,
    output logic       scl_out,
    output logic       scl_oe
);
```

## 3. Signal Requirements

### Inputs

- `clk`: System clock.
- `rst_n`: Active-low synchronous reset.
- `start`: Requests a new transaction while the controller is idle.
- `slave_addr`: 7-bit I2C slave address.
- `rw`: I2C direction bit. `0` means write and `1` means read.
- `tx_data`: One byte to transmit during a write transaction.
- `sda_in`: Sampled SDA bus value.
- `scl_in`: Sampled SCL bus value.

### Outputs

- `rx_data`: Received byte for a read transaction.
- `done`: One-cycle pulse when a transaction completes.
- `busy`: High while a transaction is active.
- `ack_error`: High when an expected slave ACK is not observed.
- `sda_out`: Data value associated with the open-drain SDA output.
- `sda_oe`: SDA drive-enable control.
- `scl_out`: Clock value associated with the SCL output.
- `scl_oe`: SCL drive-enable control.

## 4. I2C Address Format

I2C uses a 7-bit slave address followed by the R/W bit.

The transmitted address byte is:

```text
bit 7                 bit 1 bit 0
+---------------------+-----+-----+
| 7-bit slave address | R/W |
+---------------------+-----+-----+
```

Numerically:

```text
address_byte = (slave_addr << 1) | rw
```

The address byte is transmitted MSB first.

## 5. Write Transaction

A write transaction shall follow:

```text
START
  |
  v
7-bit address + 0
  |
  v
Slave ACK
  |
  v
8-bit tx_data
  |
  v
Slave ACK
  |
  v
STOP
  |
  v
DONE
```

If either expected ACK is not detected (`sda_in == 1` when ACK is sampled),
`ack_error` shall be asserted.

## 6. Read Transaction

A one-byte read shall follow:

```text
START
  |
  v
7-bit address + 1
  |
  v
Slave ACK
  |
  v
8-bit data from slave
  |
  v
Master NACK
  |
  v
STOP
  |
  v
DONE
```

The received byte shall appear on `rx_data`.

For a one-byte read, the master shall send a NACK after receiving the byte,
indicating that it does not request another byte.

## 7. Transaction Control

When idle:

- `start=1` shall begin a transaction.
- `busy` shall become asserted.
- The controller shall capture the address and direction.
- A new transaction shall not overwrite an active transaction.

When the transaction reaches STOP:

- `done` shall pulse for one clock.
- `busy` shall return low.
- The controller shall return to IDLE.

## 8. Reset Requirements

When `rst_n=0`:

- `busy = 0`
- `done = 0`
- `ack_error = 0`
- `rx_data = 0`
- Controller state returns to IDLE.

## 9. Timing

`CLK_DIV` determines the number of system-clock cycles between transaction
engine state advances.

Verification should use `done` to identify transaction completion instead of
assuming a fixed latency.

## 10. Open-Drain Behavior

I2C uses open-drain/open-collector signaling.

A released line allows the external pull-up to drive the bus high.

Conceptually:

```text
Drive low:
    output-enable = 1
    driven value  = 0

Release:
    output-enable = 0
    external pull-up drives line high
```

The verification environment should therefore treat SDA and SCL as shared
bus signals rather than ordinary push-pull outputs.

## 11. Verification Requirements

A Cocotb/PyUVM environment should verify at least:

### Address generation

```text
address_byte == (slave_addr << 1) | rw
```

### Write path

- Correct START sequencing.
- Correct 7-bit address.
- Correct write R/W bit.
- MSB-first address transmission.
- Correct transmitted data byte.
- ACK detection after address.
- ACK detection after data.
- STOP generation.
- `done` assertion.
- `busy` deassertion.

### Read path

- Correct read R/W bit.
- Address ACK detection.
- Correct sampling of eight data bits.
- Correct `rx_data`.
- Master NACK after one-byte read.
- STOP generation.
- `done` assertion.

### Error handling

- Missing address ACK sets `ack_error`.
- Missing write-data ACK sets `ack_error`.
- A successful transaction leaves `ack_error` low.

### Control

- `start` while busy must not corrupt the active transaction.
- `done` must be a pulse rather than a permanent level.
- Reset must return the controller to a known idle state.

## 12. Suggested ARGUS Fault Classes

This benchmark provides useful repair targets including:

- Address/RW bit errors
- MSB/LSB ordering errors
- ACK sampling errors
- Read-data bit-order errors
- Incorrect bit-counter initialization
- Incorrect terminal conditions
- START/STOP state errors
- Busy/done protocol errors
- Reset errors
- Read/write path selection errors
- ACK error propagation errors
- State-transition errors

## 13. Scope Limitations

Not included:

- Clock stretching
- Arbitration between multiple masters
- 10-bit addressing
- Repeated START
- Multi-byte transfers
- General Call
- SMBus extensions
- Full electrical analog behavior

The benchmark is intended to test digital RTL verification and repair of
I2C transaction/control logic.
