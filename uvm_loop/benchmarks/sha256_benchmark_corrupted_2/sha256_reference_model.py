
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
    """Return the SHA-256 digest represented by one canonical padded block."""
    if len(block) != 64:
        raise ValueError("Block must be exactly 64 bytes.")

    # The final 8 bytes contain the original message length in bits.
    bit_len = int.from_bytes(block[56:64], "big")

    # This single-block benchmark only supports byte-aligned messages
    # shorter than 56 bytes.
    if bit_len % 8 != 0:
        raise ValueError("Invalid SHA-256 bit length.")

    message_len = bit_len // 8

    if message_len >= 56:
        raise ValueError(
            "Message must be shorter than 56 bytes for one-block SHA-256."
        )

    # The padding marker is determined by the encoded message length,
    # not by searching for the first 0x80 byte. The original message
    # itself may legitimately contain 0x80.
    if block[message_len] != 0x80:
        raise ValueError("Missing SHA-256 0x80 padding marker.")

    # All bytes between the 0x80 marker and the length field must be zero.
    if any(block[message_len + 1:56]):
        raise ValueError("Non-zero padding bytes.")

    # Reconstruct the original message using the encoded length.
    message = block[:message_len]

    # Verify the length field against the reconstructed message.
    if bit_len != len(message) * 8:
        raise ValueError("Invalid length field in padded block.")

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
    {
        # Explicit regression test: the message itself contains 0x80.
        "name": "embedded_0x80",
        "message": bytes.fromhex(
            "f5df83470ea569d95de9120aaaf95f4fa5e082202df03a2eb9e03cbdda802979"
        ),
        "digest": hashlib.sha256(
            bytes.fromhex(
                "f5df83470ea569d95de9120aaaf95f4fa5e082202df03a2eb9e03cbdda802979"
            )
        ).hexdigest(),
    },
]


if __name__ == "__main__":
    for vector in TEST_VECTORS:
        padded = pad_one_block(vector["message"])
        digest = block_to_digest(padded).hex()

        print(f'{vector["name"]:20s} {digest}')

        assert digest == vector["digest"]

    print("All SHA-256 reference-model tests passed.")