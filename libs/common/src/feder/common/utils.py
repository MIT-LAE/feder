from io import BytesIO
import struct


MISSING_VALUE = -999999.0


def encode_opt_float(x: float | None) -> float:
    return MISSING_VALUE if x is None else x


def decode_opt_float(x: float) -> float | None:
    return None if x == MISSING_VALUE else x


def milli(x):
    return round(x, 3)


class Packer:
    """Helper class to pack messages using Python's struct."""

    def __init__(self):
        self._buf = BytesIO()

    def data(self):
        return self._buf.getvalue()

    def __call__(self, fmt, *vals):
        self._buf.write(struct.pack(fmt, *vals))

    def str(self, s: str | None):
        if s is None:
            s = ''
        if len(s) > 255:
            raise ValueError('short string too long for packing in Message')
        self('>B', len(s))
        self._buf.write(s.encode('utf-8'))


class Unpacker:
    """Helper class to unpack messages using Python's struct."""

    def __init__(self, data: bytes):
        self._buf = BytesIO(data)

    def __call__(self, fmt: str, multiple: bool = False):
        size = struct.calcsize(fmt)
        result = struct.unpack(fmt, self._buf.read(size))
        return result if multiple else result[0]

    def iter(self, fmt: str, multiple: bool = False):
        size = struct.calcsize(fmt)
        while True:
            d = self._buf.read(size)
            if len(d) < size:
                break
            result = struct.unpack(fmt, d)
            yield result if multiple else result[0]

    def str(self) -> str | None:
        length = self('>B')
        if length == 0:
            return None
        return self._buf.read(length).decode()
