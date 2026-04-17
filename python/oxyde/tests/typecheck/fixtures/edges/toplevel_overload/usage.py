from __future__ import annotations

from module import Record, resolve


def main() -> None:
    a: str = resolve(1)
    b: int = resolve("x")
    _ = (a, b, Record)
