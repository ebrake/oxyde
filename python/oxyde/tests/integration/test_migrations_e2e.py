"""End-to-end migration tests against real databases via testcontainers.

Each test:
    1. Builds one or two migration files in tmp_path.
    2. Applies them against a fresh DB (SQLite file / PG-container / MySQL-container).
    3. Inspects the resulting schema via information_schema / pg_catalog / sqlite_master.

Table names are suffixed with a per-test UUID so parallel test runs against the
shared PG/MySQL containers do not collide.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest
import pytest_asyncio

from oxyde import AsyncDatabase, Field, Index, Model, disconnect_all
from oxyde.migrations import (
    apply_migrations,
    extract_current_schema,
    generate_migration_file,
    rollback_migrations,
)
from oxyde.migrations.utils import detect_dialect
from oxyde.models.registry import clear_registry, register_table
from oxyde.queries.raw import execute_raw
from oxyde.tests.integration.conftest import _get_url


@pytest_asyncio.fixture(params=["sqlite", "postgres", "mysql"])
async def empty_db(request, tmp_path, _pg_container, _mysql_container):
    """Fresh DB with no tables — unlike `db` fixture that seeds ALL_MODELS."""
    url = _get_url(request.param, tmp_path, _pg_container, _mysql_container)
    db_name = f"mig_{request.param}_{uuid.uuid4().hex[:8]}"
    database = AsyncDatabase(url, name=db_name, overwrite=True)
    await database.connect()

    clear_registry()

    yield database, request.param
    await disconnect_all()


async def _table_exists(db_name: str, table: str, dialect: str) -> bool:
    if dialect == "postgres":
        rows = await execute_raw(
            "SELECT 1 FROM information_schema.tables WHERE table_name = $1",
            [table],
            using=db_name,
        )
    elif dialect == "mysql":
        rows = await execute_raw(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = ?",
            [table],
            using=db_name,
        )
    else:  # sqlite
        rows = await execute_raw(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            [table],
            using=db_name,
        )
    return bool(rows)


async def _column_exists(
    db_name: str, table: str, column: str, dialect: str
) -> bool:
    if dialect == "postgres":
        rows = await execute_raw(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = $1 AND column_name = $2",
            [table, column],
            using=db_name,
        )
    elif dialect == "mysql":
        rows = await execute_raw(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?",
            [table, column],
            using=db_name,
        )
    else:  # sqlite
        rows = await execute_raw(
            f"SELECT 1 FROM pragma_table_info('{table}') WHERE name = ?",
            [column],
            using=db_name,
        )
    return bool(rows)


async def _fk_exists(db_name: str, fk_name: str, dialect: str) -> bool:
    """Check if a named FK constraint exists. Postgres/MySQL only."""
    if dialect == "postgres":
        rows = await execute_raw(
            "SELECT 1 FROM pg_constraint WHERE conname = $1",
            [fk_name],
            using=db_name,
        )
    elif dialect == "mysql":
        rows = await execute_raw(
            "SELECT 1 FROM information_schema.table_constraints "
            "WHERE constraint_schema = DATABASE() AND constraint_name = ?",
            [fk_name],
            using=db_name,
        )
    else:
        pytest.skip("FK introspection not implemented for sqlite")
    return bool(rows)


async def _index_exists(db_name: str, index_name: str, dialect: str) -> bool:
    if dialect == "postgres":
        rows = await execute_raw(
            "SELECT 1 FROM pg_indexes WHERE indexname = $1",
            [index_name],
            using=db_name,
        )
    elif dialect == "mysql":
        rows = await execute_raw(
            "SELECT 1 FROM information_schema.statistics "
            "WHERE table_schema = DATABASE() AND index_name = ?",
            [index_name],
            using=db_name,
        )
    else:  # sqlite
        rows = await execute_raw(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
            [index_name],
            using=db_name,
        )
    return bool(rows)


def _write_migration(
    tmp_path: Path,
    old_models: list[type[Model]],
    new_models: list[type[Model]],
    dialect: str,
    name: str,
) -> Path:
    import json

    from oxyde.core import migration_compute_diff

    clear_registry()
    for m in old_models:
        register_table(m, overwrite=True)
    old = extract_current_schema(dialect=dialect)

    clear_registry()
    for m in new_models:
        register_table(m, overwrite=True)
    new = extract_current_schema(dialect=dialect)

    ops_json = migration_compute_diff(json.dumps(old), json.dumps(new))
    ops = json.loads(ops_json)
    return generate_migration_file(ops, migrations_dir=tmp_path, name=name)


class TestCreateTableE2E:
    @pytest.mark.asyncio
    async def test_create_and_drop_table(self, empty_db, tmp_path):
        database, dialect = empty_db
        suffix = uuid.uuid4().hex[:8]
        tbl = f"users_{suffix}"

        class UserV1(Model):
            id: int | None = Field(default=None, db_pk=True)
            email: str = Field(max_length=255)

            class Meta:
                is_table = True
                table_name = tbl

        _write_migration(tmp_path, [], [UserV1], dialect, f"create_{suffix}")
        await apply_migrations(migrations_dir=str(tmp_path), db_alias=database.name)
        assert await _table_exists(database.name, tbl, dialect)

        _write_migration(tmp_path, [UserV1], [], dialect, f"drop_{suffix}")
        await apply_migrations(migrations_dir=str(tmp_path), db_alias=database.name)
        assert not await _table_exists(database.name, tbl, dialect)


class TestAddDropColumnE2E:
    @pytest.mark.asyncio
    async def test_add_column(self, empty_db, tmp_path):
        database, dialect = empty_db
        suffix = uuid.uuid4().hex[:8]
        tbl = f"users_{suffix}"

        class UserV1(Model):
            id: int | None = Field(default=None, db_pk=True)
            email: str = Field(max_length=255)

            class Meta:
                is_table = True
                table_name = tbl

        class UserV2(Model):
            id: int | None = Field(default=None, db_pk=True)
            email: str = Field(max_length=255)
            nickname: str | None = Field(
                default=None, db_nullable=True, max_length=100
            )

            class Meta:
                is_table = True
                table_name = tbl

        _write_migration(tmp_path, [], [UserV1], dialect, f"create_{suffix}")
        _write_migration(tmp_path, [UserV1], [UserV2], dialect, f"add_nick_{suffix}")

        await apply_migrations(migrations_dir=str(tmp_path), db_alias=database.name)
        assert await _column_exists(database.name, tbl, "nickname", dialect)

    @pytest.mark.asyncio
    async def test_drop_column(self, empty_db, tmp_path):
        database, dialect = empty_db
        suffix = uuid.uuid4().hex[:8]
        tbl = f"users_{suffix}"

        class UserV1(Model):
            id: int | None = Field(default=None, db_pk=True)
            email: str = Field(max_length=255)
            deprecated: str | None = Field(
                default=None, db_nullable=True, max_length=100
            )

            class Meta:
                is_table = True
                table_name = tbl

        class UserV2(Model):
            id: int | None = Field(default=None, db_pk=True)
            email: str = Field(max_length=255)

            class Meta:
                is_table = True
                table_name = tbl

        _write_migration(tmp_path, [], [UserV1], dialect, f"create_{suffix}")
        _write_migration(
            tmp_path, [UserV1], [UserV2], dialect, f"drop_deprecated_{suffix}"
        )

        if dialect == "sqlite":
            # SQLite needs rebuild for ALTER COLUMN / DROP COLUMN combined —
            # for plain drop_column on a simple table it does support it since 3.35.
            pass

        await apply_migrations(migrations_dir=str(tmp_path), db_alias=database.name)
        assert not await _column_exists(
            database.name, tbl, "deprecated", dialect
        )
        assert await _column_exists(database.name, tbl, "email", dialect)


class TestIndexE2E:
    @pytest.mark.asyncio
    async def test_create_and_drop_index(self, empty_db, tmp_path):
        database, dialect = empty_db
        suffix = uuid.uuid4().hex[:8]
        tbl = f"users_{suffix}"
        idx_name = f"idx_{tbl}_email"

        class UserV1(Model):
            id: int | None = Field(default=None, db_pk=True)
            email: str = Field(max_length=255)

            class Meta:
                is_table = True
                table_name = tbl

        class UserV2(Model):
            id: int | None = Field(default=None, db_pk=True)
            email: str = Field(max_length=255)

            class Meta:
                is_table = True
                table_name = tbl
                indexes = [Index(fields=["email"], name=idx_name)]

        _write_migration(tmp_path, [], [UserV1], dialect, f"create_{suffix}")
        _write_migration(tmp_path, [UserV1], [UserV2], dialect, f"add_idx_{suffix}")
        _write_migration(tmp_path, [UserV2], [UserV1], dialect, f"drop_idx_{suffix}")

        # Apply first two — index should exist
        await apply_migrations(
            migrations_dir=str(tmp_path),
            db_alias=database.name,
            target="0002",
        )
        assert await _index_exists(database.name, idx_name, dialect)

        # Apply the third — index gone
        await apply_migrations(migrations_dir=str(tmp_path), db_alias=database.name)
        assert not await _index_exists(database.name, idx_name, dialect)


class TestDropForeignKeyBugRegressionE2E:
    """The exact scenario from the bug report, driven end-to-end against a real DB.

    Postgres/MySQL only — SQLite doesn't support ALTER TABLE DROP FK.
    """

    @pytest_asyncio.fixture(params=["postgres", "mysql"])
    async def pg_or_mysql_db(
        self, request, tmp_path, _pg_container, _mysql_container
    ):
        url = _get_url(request.param, tmp_path, _pg_container, _mysql_container)
        db_name = f"fk_{request.param}_{uuid.uuid4().hex[:8]}"
        database = AsyncDatabase(url, name=db_name, overwrite=True)
        await database.connect()
        clear_registry()
        yield database, request.param
        await disconnect_all()

    @pytest.mark.asyncio
    async def test_remove_fk_keep_column(self, pg_or_mysql_db, tmp_path):
        database, dialect = pg_or_mysql_db
        suffix = uuid.uuid4().hex[:8]
        authors = f"authors_{suffix}"
        books = f"books_{suffix}"
        fk_name = f"fk_{books}_author_id"

        class AuthorV1(Model):
            id: int | None = Field(default=None, db_pk=True)
            name: str = Field(max_length=100)

            class Meta:
                is_table = True
                table_name = authors

        class BookV1(Model):
            id: int | None = Field(default=None, db_pk=True)
            title: str = Field(max_length=200)
            author: AuthorV1 | None = Field(default=None, db_on_delete="CASCADE")

            class Meta:
                is_table = True
                table_name = books

        class AuthorV2(Model):
            id: int | None = Field(default=None, db_pk=True)
            name: str = Field(max_length=100)

            class Meta:
                is_table = True
                table_name = authors

        class BookV2(Model):
            id: int | None = Field(default=None, db_pk=True)
            title: str = Field(max_length=200)
            author_id: int | None = Field(default=None, db_nullable=True)

            class Meta:
                is_table = True
                table_name = books

        _write_migration(
            tmp_path, [], [AuthorV1, BookV1], dialect, f"initial_{suffix}"
        )
        _write_migration(
            tmp_path,
            [AuthorV1, BookV1],
            [AuthorV2, BookV2],
            dialect,
            f"drop_fk_{suffix}",
        )

        # First migration creates tables + FK
        await apply_migrations(
            migrations_dir=str(tmp_path),
            db_alias=database.name,
            target="0001",
        )
        assert await _fk_exists(database.name, fk_name, dialect)

        # Second migration drops the FK — this is where the bug manifested
        await apply_migrations(migrations_dir=str(tmp_path), db_alias=database.name)
        assert not await _fk_exists(database.name, fk_name, dialect)
        assert await _column_exists(database.name, books, "author_id", dialect)


class TestUpgradeDowngradeSymmetryE2E:
    @pytest.mark.asyncio
    async def test_rollback_restores_state(self, empty_db, tmp_path):
        database, dialect = empty_db
        suffix = uuid.uuid4().hex[:8]
        tbl = f"users_{suffix}"

        class UserV1(Model):
            id: int | None = Field(default=None, db_pk=True)
            email: str = Field(max_length=255)

            class Meta:
                is_table = True
                table_name = tbl

        class UserV2(Model):
            id: int | None = Field(default=None, db_pk=True)
            email: str = Field(max_length=255)
            nickname: str | None = Field(
                default=None, db_nullable=True, max_length=100
            )

            class Meta:
                is_table = True
                table_name = tbl

        _write_migration(tmp_path, [], [UserV1], dialect, f"create_{suffix}")
        _write_migration(tmp_path, [UserV1], [UserV2], dialect, f"add_nick_{suffix}")

        await apply_migrations(migrations_dir=str(tmp_path), db_alias=database.name)
        assert await _column_exists(database.name, tbl, "nickname", dialect)

        await rollback_migrations(
            steps=1, migrations_dir=str(tmp_path), db_alias=database.name
        )
        assert not await _column_exists(database.name, tbl, "nickname", dialect)
        assert await _column_exists(database.name, tbl, "email", dialect)
