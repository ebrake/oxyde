"""Edge: module-level @overload function alongside a Model."""

from __future__ import annotations

from typing import overload

from oxyde import Field, Model


class Record(Model):
    id: int | None = Field(default=None, db_pk=True)
    name: str = Field(default="")

    class Meta:
        is_table = True


@overload
def resolve(x: int) -> str: ...
@overload
def resolve(x: str) -> int: ...
def resolve(x: int | str) -> int | str:
    return str(x) if isinstance(x, int) else int(x)
