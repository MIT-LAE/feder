from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Self

from feder_common.utils import Packer, Unpacker


@dataclass
class TestMessage(ABC):
    MESSAGE_TAG = None

    @staticmethod
    def unpack(data: bytes) -> Self:
        unpacker = Unpacker(data)
        tag = chr(unpacker('>B'))
        message_class = MESSAGE_TAG_DICT.get(tag)
        if message_class is None:
            raise ValueError(
                f'unknown message tag "{tag}" for message'
            )
        return message_class._unpack(unpacker)

    def pack(self) -> bytes:
        packer = Packer()
        packer('>B', ord(self.MESSAGE_TAG))
        self._pack(packer)
        return packer.data()

    @abstractmethod
    def _pack(self, packer: Packer):
        ...

    @classmethod
    @abstractmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        ...


@dataclass
class PubTest(TestMessage):
    MESSAGE_TAG = 'P'

    name: str

    def _pack(self, packer: Packer):
        packer.str(self.name)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(
            name=unpacker.str()
        )

@dataclass
class FibonacciRequest(TestMessage):
    MESSAGE_TAG = 'F'

    name: str
    data: int

    def _pack(self, packer: Packer):
        packer.str(self.name)
        packer('>I', self.data)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(
            name=unpacker.str(),
            data=unpacker('>I')
        )

@dataclass
class FibonacciResponse(TestMessage):
    MESSAGE_TAG = 'f'

    name: str
    data: int
    success: bool

    def _pack(self, packer: Packer):
        packer.str(self.name)
        packer('>I', self.data)
        packer('>?', self.success)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(
            name=unpacker.str(),
            data=unpacker('>I'),
            success=unpacker('>?')
        )

@dataclass
class FactorialRequest(TestMessage):
    MESSAGE_TAG = 'N'

    name: str
    data: int

    def _pack(self, packer: Packer):
        packer.str(self.name)
        packer('>I', self.data)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(
            name=unpacker.str(),
            data=unpacker('>I')
        )

@dataclass
class FactorialResponse(TestMessage):
    MESSAGE_TAG = 'n'

    name: str
    data: int
    success: bool

    def _pack(self, packer: Packer):
        packer.str(self.name)
        packer('>I', self.data)
        packer('>?', self.success)

    @classmethod
    def _unpack(cls, unpacker: Unpacker) -> Self:
        return cls(
            name=unpacker.str(),
            data=unpacker('>I'),
            success=unpacker('>?')
        )

MESSAGE_TAG_DICT = {
    c.MESSAGE_TAG: c for c in [
        PubTest, FibonacciRequest, FibonacciResponse,
        FactorialRequest, FactorialResponse
    ]
}
