import hashlib
from pathlib import Path

class _builtin_hash:
    def __init__(self):
        self._value = 0

    def update(self, data: bytes):
        self._value = hash((self._value, data))

    def hexdigest(self) -> str:
        return f"{self._value & 0xFFFFFFFFFFFFFFFF:016x}"

def hash_file(path: Path, algorithm: str, chunk_size: int | None = None, max_read: int | None = None) -> str:
    with open(path, 'rb') as f:
        # Do not read the whole file into memory at once
        # Use the specified algorithm to hash the file
        algorithm = algorithm.lower()
        if algorithm in ("py", "python"):
            hash_alg = _builtin_hash()
        elif algorithm in hashlib.algorithms_available:
            hash_alg = hashlib.new(algorithm)
        else:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")

        read = 0
        while (chunk := f.read(chunk_size or 8192)):
            hash_alg.update(chunk)
            read += len(chunk)
            if max_read is not None and read >= max_read:
                break
        return hash_alg.hexdigest()

def read_16b(file: Path) -> str:
    # Read the first 64 bytes of a file, and mash it to get 16 bytes which should be enough to distinguish files.
    with open(file, "rb") as f:
        bs = [bytearray(f.read(16).ljust(16, b'\0')) for _ in range(4)]

    res = bytearray(16)

    for i in range(16):
        val = 0
        for j, chunk in enumerate(bs):
            # Rotate each byte by (i + j) bits and XOR into accumulator
            n = (i + j) % 8
            rotated = ((chunk[i] << n) & 0xFF) | (chunk[i] >> (8 - n))
            val ^= rotated
        res[i] = val

    # Return as hex string
    return res.hex()