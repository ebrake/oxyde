from __future__ import annotations

from module import Note, fetch_recent_notes, format_note


async def main() -> None:
    notes: list[Note] = await fetch_recent_notes(limit=5)
    for n in notes:
        line: str = format_note(n)
        print(line)
