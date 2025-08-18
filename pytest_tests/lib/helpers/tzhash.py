"""
Pure Python implementation of Tillich-Zémor hash function.

Based on the Go implementation from https://github.com/nspcc-dev/tzhash
"""

import struct
from typing import List, Optional


class GF127:
    """Element of GF(2^127) with reduction polynomial x^127 + x^63 + 1."""

    SIZE = 16
    MSB64 = 1 << 63
    MAX_UINT64 = (1 << 64) - 1

    def __init__(self, lo: int = 0, hi: int = 0):
        self.lo = lo & self.MAX_UINT64
        self.hi = hi & ((1 << 63) - 1)  # Ensure MSB is zero

    def __eq__(self, other: "GF127") -> bool:
        return self.lo == other.lo and self.hi == other.hi

    def __repr__(self) -> str:
        return f"GF127(lo={self.lo:016x}, hi={self.hi:015x})"

    @classmethod
    def from_bytes(cls, data: bytes) -> "GF127":
        if len(data) != cls.SIZE:
            raise ValueError("Data must be 16 bytes long")

        hi = struct.unpack(">Q", data[:8])[0]
        lo = struct.unpack(">Q", data[8:])[0]

        if hi & cls.MSB64:
            raise ValueError("MSB must be zero")

        return cls(lo, hi)

    def to_bytes(self) -> bytes:
        return struct.pack(">QQ", self.hi, self.lo)

    def copy(self) -> "GF127":
        return GF127(self.lo, self.hi)

    def add(self, other: "GF127") -> "GF127":
        return GF127(self.lo ^ other.lo, self.hi ^ other.hi)

    def mul10(self) -> "GF127":
        c = self.lo >> 63
        lo = (self.lo << 1) & self.MAX_UINT64
        hi = ((self.hi << 1) ^ c) & self.MAX_UINT64

        mask = hi & self.MSB64
        lo ^= mask | (mask >> 63)
        hi ^= mask
        hi &= (1 << 63) - 1  # Clear MSB

        return GF127(lo, hi)

    def mul11(self) -> "GF127":
        c = self.lo >> 63
        lo = self.lo ^ ((self.lo << 1) & self.MAX_UINT64)
        hi = self.hi ^ ((self.hi << 1) & self.MAX_UINT64) ^ c

        mask = hi & self.MSB64
        lo ^= mask | (mask >> 63)
        hi ^= mask
        hi &= (1 << 63) - 1  # Clear MSB

        return GF127(lo, hi)

    def multiply(self, other: "GF127") -> "GF127":
        result = GF127()
        d = self.copy()

        for i in range(64):
            if other.lo & (1 << i):
                result = result.add(d)
            d = d.mul10()

        for i in range(63):
            if other.hi & (1 << i):
                result = result.add(d)
            d = d.mul10()

        return result

    def inverse(self) -> "GF127":
        v = GF127(self.MSB64 + 1, self.MSB64 >> 1)
        u = self.copy()
        c = GF127(1, 0)
        d = GF127(0, 0)

        def msb_pos(x: GF127) -> int:
            if x.hi == 0:
                if x.lo == 0:
                    return 0
                return 63 - (x.lo.bit_length() - 1) if x.lo > 0 else 0
            return 127 - (x.hi.bit_length() - 1) if x.hi > 0 else 64

        def x_power_n(n: int) -> GF127:
            if n < 64:
                return GF127(1 << n, 0)
            return GF127(0, 1 << (n - 64))

        du = msb_pos(u)
        dv = msb_pos(v)

        while du != 0:
            if du < dv:
                u, v = v, u
                du, dv = dv, du
                c, d = d, c

            x_elem = x_power_n(du - dv)

            t = x_elem.multiply(v)
            u = u.add(t)

            # Manual reduction if needed
            if msb_pos(u) == 127:
                reduction = GF127(self.MSB64 + 1, self.MSB64 >> 1)
                u = u.add(reduction)

            t = x_elem.multiply(d)
            c = c.add(t)

            du = msb_pos(u)

        return c


class SL2:
    def __init__(self, matrix: Optional[List[List[GF127]]] = None):
        if matrix is None:
            # Identity matrix
            self.matrix = [[GF127(1, 0), GF127(0, 0)], [GF127(0, 0), GF127(1, 0)]]
        else:
            self.matrix = [[elem.copy() for elem in row] for row in matrix]

    def copy(self) -> "SL2":
        return SL2(self.matrix)

    def multiply(self, other: "SL2") -> "SL2":
        result = SL2()

        result.matrix[0][0] = (
            self.matrix[0][0].multiply(other.matrix[0][0]).add(self.matrix[0][1].multiply(other.matrix[1][0]))
        )
        result.matrix[0][1] = (
            self.matrix[0][0].multiply(other.matrix[0][1]).add(self.matrix[0][1].multiply(other.matrix[1][1]))
        )
        result.matrix[1][0] = (
            self.matrix[1][0].multiply(other.matrix[0][0]).add(self.matrix[1][1].multiply(other.matrix[1][0]))
        )
        result.matrix[1][1] = (
            self.matrix[1][0].multiply(other.matrix[0][1]).add(self.matrix[1][1].multiply(other.matrix[1][1]))
        )

        return result

    def inverse(self) -> "SL2":
        # For 2x2 matrix [[a,b],[c,d]], inverse is (1/det) * [[d,-b],[-c,a]]
        # where det = ad - bc
        a, b = self.matrix[0][0], self.matrix[0][1]
        c, d = self.matrix[1][0], self.matrix[1][1]

        det = a.multiply(d).add(b.multiply(c))
        det_inv = det.inverse()

        result = SL2()
        result.matrix[0][0] = det_inv.multiply(d)
        result.matrix[0][1] = det_inv.multiply(b)
        result.matrix[1][0] = det_inv.multiply(c)
        result.matrix[1][1] = det_inv.multiply(a)

        return result

    def to_bytes(self) -> bytes:
        result = b""
        for row in self.matrix:
            for elem in row:
                result += elem.to_bytes()
        return result

    @classmethod
    def from_bytes(cls, data: bytes) -> "SL2":
        if len(data) != 64:
            raise ValueError("Data must be 64 bytes long")

        matrix = [[None, None], [None, None]]
        for i in range(2):
            for j in range(2):
                start = (i * 2 + j) * 16
                end = start + 16
                matrix[i][j] = GF127.from_bytes(data[start:end])

        return cls(matrix)


class TZHash:
    """Tillich-Zémor hash implementation."""

    SIZE = 64
    BLOCK_SIZE = 128

    def __init__(self):
        self.reset()

    def reset(self):
        # Matrix stored as [x[0], x[2]]
        #                  [x[1], x[3]]
        self.x = [
            GF127(1, 0),  # x[0] - top-left
            GF127(0, 0),  # x[1] - bottom-left
            GF127(0, 0),  # x[2] - top-right
            GF127(1, 0),  # x[3] - bottom-right
        ]

    def _mul_bit_right(self, bit: bool):
        if bit:
            # Multiply by matrix [[x, x+1], [1, x]]
            # For bit = 1: multiply by [[x, x+1], [1, x]]
            tmp = self.x[0].copy()
            self.x[0] = self.x[0].mul10().add(self.x[2])
            self.x[2] = tmp.mul11().add(self.x[2])

            tmp = self.x[1].copy()
            self.x[1] = self.x[1].mul10().add(self.x[3])
            self.x[3] = tmp.mul11().add(self.x[3])
        else:
            # Multiply by matrix [[x, 1], [1, 0]]
            # For bit = 0: multiply by [[x, 1], [1, 0]]
            tmp = self.x[0].copy()
            self.x[0] = self.x[0].mul10().add(self.x[2])
            self.x[2] = tmp

            tmp = self.x[1].copy()
            self.x[1] = self.x[1].mul10().add(self.x[3])
            self.x[3] = tmp

    def update(self, data: bytes) -> int:
        for byte in data:
            # Process each bit from MSB to LSB
            for i in range(8):
                bit = (byte & (0x80 >> i)) != 0
                self._mul_bit_right(bit)
        return len(data)

    def digest(self) -> bytes:
        result = bytearray(64)

        # Pack matrix elements in the correct order
        result[0:16] = self.x[0].to_bytes()  # top-left
        result[16:32] = self.x[2].to_bytes()  # top-right
        result[32:48] = self.x[1].to_bytes()  # bottom-left
        result[48:64] = self.x[3].to_bytes()  # bottom-right

        return bytes(result)

    def hexdigest(self) -> str:
        return self.digest().hex()

    @staticmethod
    def hash_data(data: bytes) -> bytes:
        h = TZHash()
        h.update(data)
        return h.digest()

    @staticmethod
    def concat_hashes(hashes: List[bytes]) -> bytes:
        if not hashes:
            raise ValueError("Empty hash list")

        result = SL2()  # Identity matrix

        for hash_bytes in hashes:
            if len(hash_bytes) != TZHash.SIZE:
                raise ValueError(f"Invalid hash size: {len(hash_bytes)}")

            matrix = SL2.from_bytes(hash_bytes)
            result = result.multiply(matrix)

        return result.to_bytes()

    @staticmethod
    def validate_hashes(combined_hash: bytes, individual_hashes: List[bytes]) -> bool:
        try:
            computed = TZHash.concat_hashes(individual_hashes)
            return computed == combined_hash
        except Exception:
            return False


def main():
    """Demo of TZ hash functionality."""
    print("Tillich-Zémor Hash Demo")
    print("=" * 30)

    test_data = b"Hello, World!"
    h = TZHash()
    h.update(test_data)
    hash1 = h.digest()

    print(f"Input: {test_data}")
    print(f"Hash:  {hash1.hex()}")

    data1 = b"Hello, "
    data2 = b"World!"

    hash_part1 = TZHash.hash_data(data1)
    hash_part2 = TZHash.hash_data(data2)
    combined_hash = TZHash.concat_hashes([hash_part1, hash_part2])

    print("\nHomomorphic property test:")
    print(f"Part 1: {data1} -> {hash_part1.hex()}")
    print(f"Part 2: {data2} -> {hash_part2.hex()}")
    print(f"Combined: {combined_hash.hex()}")
    print(f"Direct hash: {hash1.hex()}")
    print(f"Match: {combined_hash == hash1}")

    is_valid = TZHash.validate_hashes(hash1, [hash_part1, hash_part2])
    print(f"Validation: {is_valid}")


if __name__ == "__main__":
    main()
