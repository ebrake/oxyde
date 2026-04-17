"""Edge: module-level helper functions alongside a Model."""

from __future__ import annotations

from oxyde import Field, Model


class Note(Model):
    id: int | None = Field(default=None, db_pk=True)
    text: str = Field(default="")

    class Meta:
        is_table = True


async def fetch_recent_notes(limit: int = 10) -> list[Note]:
    return await Note.objects.all()


def format_note(note: Note) -> str:
    return f"{note.id}: {note.text}"
