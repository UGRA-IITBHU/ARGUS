"""Reference model for the ARGUS SHA-256 single-block benchmark.

The DUT accepts one already-padded 512-bit SHA-256 message block.
This reference model uses Python's hashlib as an independent golden model.
"""

import hashlib


def pad_one_block(message: bytes) -> bytes:
    """SHA-256 pad a message that fits into exactly one 512-bit block."""
    if len(message) >= 56:
        raise ValueError("Message must be shorter than 56 bytes for one block.")

    bit_len = len(message) * 8
    padded = message + b"\x80"
    padded += b"\x00" * (56 - len(padded))
    padded += bit_len.to_bytes(8, "big")

    assert len(padded) == 64
    return padded


def sha256_digest(message: bytes) -> bytes:
    """Return the standard SHA-256 digest."""
    if len(message) >= 56:
        raise ValueError("This benchmark reference model is single-block.")
    return hashlib.sha256(message).digest()


def sha256_hex(message: bytes) -> str:
    return sha256_digest(message).hex()


def block_to_digest(block: bytes) -> bytes:
    """Return the SHA-256 digest represented by one padded 64-byte block."""
    if len(block) != 64:
        raise ValueError("Block must be exactly 64 bytes.")

    # Reconstruct the original message represented by the one-block padding.
    if block[0] == 0x80:
        message = b""
    else:
        try:
            marker = block.index(b"\x80")
        except ValueError as exc:
            raise ValueError("Invalid SHA-256 padded block.") from exc
        message = block[:marker]

    # For this benchmark, verify the canonical padding layout.
    bit_len = int.from_bytes(block[56:64], "big")
    if bit_len != len(message) * 8:
        raise ValueError("Invalid length field in padded block.")
    if block[len(message)] != 0x80:
        raise ValueError("Missing SHA-256 0x80 padding marker.")
    if any(block[len(message) + 1:56]):
        raise ValueError("Non-zero padding bytes.")
    if len(block) != 64:
        raise ValueError("Invalid block size.")

    return sha256_digest(message)


# Known-answer tests useful for ARGUS-generated verification.
TEST_VECTORS = [
    {
        "name": "empty",
        "message": b"",
        "digest": hashlib.sha256(b"").hexdigest(),
    },
    {
        "name": "abc",
        "message": b"abc",
        "digest": hashlib.sha256(b"abc").hexdigest(),
    },
    {
        "name": "hello_world",
        "message": b"hello world",
        "digest": hashlib.sha256(b"hello world").hexdigest(),
    },
    {
        "name": "quick_brown_fox",
        "message": b"The quick brown fox jumps over the lazy dog",
        "digest": hashlib.sha256(
            b"The quick brown fox jumps over the lazy dog"
        ).hexdigest(),
    },
    {
        "name": "single_byte",
        "message": b"a",
        "digest": hashlib.sha256(b"a").hexdigest(),
    },
]


if __name__ == "__main__":
    for vector in TEST_VECTORS:
        padded = pad_one_block(vector["message"])
        digest = block_to_digest(padded).hex()
        print(f'{vector["name"]:20s} {digest}')
        assert digest == vector["digest"]

    print("All SHA-256 reference-model tests passed.")
