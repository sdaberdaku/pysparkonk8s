"""Sizing value types that render to Kubernetes quantities and Spark/JVM size strings."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from functools import total_ordering

_NUMBER = r"(?P<value>\d+(?:\.\d*)?|\.\d+)"
_K8S_CPU = re.compile(rf"{_NUMBER}(?P<unit>m?)")
_MIB = 2**20

_K8S_UNITS = {
    "Ei": 2**60, "Pi": 2**50, "Ti": 2**40, "Gi": 2**30, "Mi": 2**20, "Ki": 2**10,
    "E": 10**18, "P": 10**15, "T": 10**12, "G": 10**9, "M": 10**6, "k": 10**3,
    "": 1,
}  # fmt: skip
_SPARK_UNITS = {"b": 1} | {
    prefix + suffix: 2**shift
    for prefix, shift in (("k", 10), ("m", 20), ("g", 30), ("t", 40), ("p", 50))
    for suffix in ("", "b")
}


def _parse_size(text: str, units: dict[str, int], kind: str) -> int:
    pattern = rf"{_NUMBER}(?P<unit>{'|'.join(sorted(units, key=len, reverse=True))})"
    match = re.fullmatch(pattern, text) if isinstance(text, str) else None
    if match is None:
        raise ValueError(f"{text!r} is not a valid {kind} memory quantity")
    return round(float(match["value"]) * units[match["unit"]])


@total_ordering
@dataclass(frozen=True)
class CPU:
    """CPU amount stored as whole millicores."""

    milli: int

    def __post_init__(self) -> None:
        if self.milli < 0:
            raise ValueError("CPU must not be negative")

    @classmethod
    def cores(cls, x: float) -> CPU:
        return cls(round(x * 1000))

    @classmethod
    def milli_cores(cls, m: int) -> CPU:
        return cls(m)

    @classmethod
    def from_k8s_spec(cls, quantity: str | int | float) -> CPU:
        if isinstance(quantity, (int, float)) and not isinstance(quantity, bool):
            return cls.cores(quantity)
        match = _K8S_CPU.fullmatch(quantity) if isinstance(quantity, str) else None
        if match is None:
            raise ValueError(f"{quantity!r} is not a valid Kubernetes CPU quantity")
        value = float(match["value"])
        return cls(round(value if match["unit"] else value * 1000))

    def to_k8s_spec(self) -> str:
        return str(self.milli // 1000) if self.milli % 1000 == 0 else f"{self.milli}m"

    def to_spark_spec(self) -> str:
        return f"{self.milli / 1000:.3f}".rstrip("0").rstrip(".")

    def __add__(self, other: CPU) -> CPU:
        if not isinstance(other, CPU):
            return NotImplemented
        return CPU(self.milli + other.milli)

    def __mul__(self, factor: float) -> CPU:
        if not isinstance(factor, (int, float)):
            return NotImplemented
        return CPU(round(self.milli * factor))

    def __lt__(self, other: CPU) -> bool:
        if not isinstance(other, CPU):
            return NotImplemented
        return self.milli < other.milli

    def __str__(self) -> str:
        return self.to_k8s_spec()


@total_ordering
@dataclass(frozen=True)
class Memory:
    """Memory amount stored as whole bytes."""

    bytes: int

    def __post_init__(self) -> None:
        if self.bytes < 0:
            raise ValueError("Memory must not be negative")

    @classmethod
    def gibibytes(cls, x: float) -> Memory:
        return cls(round(x * 2**30))

    @classmethod
    def mebibytes(cls, x: float) -> Memory:
        return cls(round(x * 2**20))

    @classmethod
    def gigabytes(cls, x: float) -> Memory:
        return cls(round(x * 10**9))

    @classmethod
    def megabytes(cls, x: float) -> Memory:
        return cls(round(x * 10**6))

    @classmethod
    def from_k8s_spec(cls, quantity: str | int) -> Memory:
        if isinstance(quantity, int) and not isinstance(quantity, bool):
            return cls(quantity)
        return cls(_parse_size(quantity, _K8S_UNITS, "Kubernetes"))

    @classmethod
    def from_spark_spec(cls, quantity: str) -> Memory:
        text = quantity.lower() if isinstance(quantity, str) else quantity
        return cls(_parse_size(text, _SPARK_UNITS, "Spark"))

    def _mebibytes_up(self) -> int:
        return math.ceil(self.bytes / _MIB)

    def to_k8s_spec(self) -> str:
        return f"{self._mebibytes_up()}Mi"

    def to_spark_spec(self) -> str:
        return f"{self._mebibytes_up()}m"

    def __add__(self, other: Memory) -> Memory:
        if not isinstance(other, Memory):
            return NotImplemented
        return Memory(self.bytes + other.bytes)

    def __mul__(self, factor: float) -> Memory:
        if not isinstance(factor, (int, float)):
            return NotImplemented
        return Memory(round(self.bytes * factor))

    def __lt__(self, other: Memory) -> bool:
        if not isinstance(other, Memory):
            return NotImplemented
        return self.bytes < other.bytes

    def __str__(self) -> str:
        return self.to_k8s_spec()
