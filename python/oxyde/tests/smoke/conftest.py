"""Smoke test fixtures — real SQLite file DB."""
from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest_asyncio

from oxyde import AsyncDatabase, disconnect_all


def _prepare_db(path: str | Path) -> None:
    """Create schema and seed data via stdlib sqlite3."""
    path_obj = Path(path) if isinstance(path, str) else path
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")

    # Create all tables first
    conn.execute(
        "CREATE TABLE IF NOT EXISTS articles (id INTEGER PRIMARY KEY, title TEXT NOT NULL, views INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS authors (id INTEGER PRIMARY KEY, email TEXT NOT NULL, name TEXT NOT NULL)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            author_id INTEGER NOT NULL,
            views INTEGER NOT NULL,
            FOREIGN KEY(author_id) REFERENCES authors(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY,
            post_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            FOREIGN KEY(post_id) REFERENCES posts(id)
        )
        """
    )

    # Delete in reverse FK order (child tables first)
    conn.execute("DELETE FROM comments")
    conn.execute("DELETE FROM posts")
    conn.execute("DELETE FROM authors")
    conn.execute("DELETE FROM articles")

    # Insert in FK order (parent tables first)
    conn.executemany(
        "INSERT INTO articles (id, title, views) VALUES (?, ?, ?)",
        [
            (1, "First", 10),
            (2, "Second", 5),
            (3, "Third", 20),
        ],
    )
    conn.executemany(
        "INSERT INTO authors (id, email, name) VALUES (?, ?, ?)",
        [
            (1, "ada@example.com", "Ada Lovelace"),
            (2, "linus@example.com", "Linus Torvalds"),
        ],
    )
    conn.executemany(
        "INSERT INTO posts (id, title, author_id, views) VALUES (?, ?, ?, ?)",
        [
            (1, "Rust Patterns", 1, 120),
            (2, "Async ORM", 1, 35),
            (3, "Kernel Notes", 2, 80),
        ],
    )
    conn.executemany(
        "INSERT INTO comments (id, post_id, body) VALUES (?, ?, ?)",
        [
            (1, 1, "Great read!"),
            (2, 1, "Thanks for sharing"),
            (3, 3, "Subscribed!"),
        ],
    )
    conn.commit()
    conn.close()


@pytest_asyncio.fixture
async def sqlite_db(tmp_path):
    """Fresh SQLite DB with seed data for each test."""
    db_path = tmp_path / "test.db"
    _prepare_db(str(db_path))

    db = AsyncDatabase(
        f"sqlite://{db_path}",
        name=f"test_{uuid.uuid4().hex}",
        overwrite=True,
    )
    await db.connect()

    try:
        yield db
    finally:
        await disconnect_all()
