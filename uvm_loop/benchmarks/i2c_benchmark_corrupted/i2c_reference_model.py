"""
Independent Python reference model for the ARGUS I2C master benchmark.

The DUT exposes a compact digital I2C master transaction interface.
This reference model describes the expected transaction-level behavior:

    START -> 7-bit address + R/W -> ACK -> data phase -> ACK/NACK -> STOP

For a write transaction:
    address + write bit -> slave ACK -> one data byte -> slave ACK -> STOP

For a read transaction:
    address + read bit -> slave ACK -> one data byte from slave
    -> master NACK -> STOP

The model does not attempt to reproduce RTL clock timing. Timing is handled
by the Cocotb/PyUVM driver/monitor; this model provides protocol expectations.
"""


def make_address_byte(slave_addr: int, read: bool) -> int:
    if not 0 <= slave_addr < 0x80:
        raise ValueError("I2C slave address must be 7 bits.")
    return (slave_addr << 1) | int(read)


def write_transaction(slave_addr: int, data: int, slave_ack: bool = True) -> dict:
    if not 0 <= data <= 0xFF:
        raise ValueError("I2C data must be one byte.")

    address_byte = make_address_byte(slave_addr, read=False)

    return {
        "start": True,
        "address": slave_addr,
        "address_byte": address_byte,
        "rw": 0,
        "tx_data": data,
        "expected_slave_acks": 2,
        "slave_ack": slave_ack,
        "expected_ack_error": not slave_ack,
        "stop": True,
    }


def read_transaction(slave_addr: int, data_from_slave: int,
                     slave_ack: bool = True) -> dict:
    if not 0 <= data_from_slave <= 0xFF:
        raise ValueError("I2C data must be one byte.")

    address_byte = make_address_byte(slave_addr, read=True)

    return {
        "start": True,
        "address": slave_addr,
        "address_byte": address_byte,
        "rw": 1,
        "tx_data": 0,
        "slave_data": data_from_slave,
        "expected_slave_acks": 1,
        "master_final_ack": False,  # NACK after a one-byte read
        "slave_ack": slave_ack,
        "expected_ack_error": not slave_ack,
        "expected_rx_data": data_from_slave,
        "stop": True,
    }


def address_bits(slave_addr: int, read: bool = False) -> list[int]:
    """Return the 8 transmitted address bits, MSB first."""
    value = make_address_byte(slave_addr, read)
    return [(value >> bit) & 1 for bit in range(7, -1, -1)]


def byte_bits(value: int) -> list[int]:
    """Return one data byte MSB first."""
    if not 0 <= value <= 0xFF:
        raise ValueError("Byte value must be 0..255.")
    return [(value >> bit) & 1 for bit in range(7, -1, -1)]


def expected_write_bytes(slave_addr: int, data: int) -> list[int]:
    return [make_address_byte(slave_addr, False), data]


def expected_read_address(slave_addr: int) -> int:
    return make_address_byte(slave_addr, True)


# Useful protocol-level tests for ARGUS-generated verification.
TEST_VECTORS = [
    {
        "name": "write_basic",
        "transaction": write_transaction(0x50, 0xA5),
    },
    {
        "name": "write_zero",
        "transaction": write_transaction(0x00, 0x00),
    },
    {
        "name": "write_max",
        "transaction": write_transaction(0x7F, 0xFF),
    },
    {
        "name": "read_basic",
        "transaction": read_transaction(0x50, 0x3C),
    },
    {
        "name": "read_max",
        "transaction": read_transaction(0x2A, 0xFF),
    },
]


if __name__ == "__main__":
    for vector in TEST_VECTORS:
        txn = vector["transaction"]

        assert txn["address_byte"] == (
            (txn["address"] << 1) | txn["rw"]
        )

        if txn["rw"] == 0:
            assert txn["tx_data"] in range(256)
        else:
            assert txn["expected_rx_data"] in range(256)
            assert txn["master_final_ack"] is False

        assert txn["start"] is True
        assert txn["stop"] is True

        print(
            f'{vector["name"]:15s} '
            f'addr=0x{txn["address"]:02x} '
            f'address_byte=0x{txn["address_byte"]:02x}'
        )

    print("All I2C reference-model tests passed.")
