"""Integration tests for pool settings (TLS, backend-specific options)."""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from oxyde import AsyncDatabase, PoolSettings, disconnect_all
from oxyde.queries.raw import execute_raw

from .conftest import _DIALECTS, _get_url


@pytest_asyncio.fixture(params=_DIALECTS)
async def pool_db(request, tmp_path, _pg_container, _mysql_container):
    """Bare database connection (no tables/seed) for pool settings tests."""
    url = _get_url(request.param, tmp_path, _pg_container, _mysql_container)
    database = AsyncDatabase(
        url, name=f"pool_{request.param}_{uuid.uuid4().hex[:6]}", overwrite=True
    )
    yield database
    await disconnect_all()


class TestPoolSettings:
    @pytest.mark.asyncio
    async def test_ssl_mode(self, pool_db):
        if pool_db.url.startswith("sqlite"):
            pytest.skip("SSL not applicable to SQLite")

        if pool_db.url.startswith("postgres"):
            settings = PoolSettings(ssl_mode="require")
            query = "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()"
        else:
            settings = PoolSettings(ssl_mode="required")
            query = "SHOW STATUS LIKE 'Ssl_cipher'"

        pool_db.settings = settings
        try:
            await pool_db.connect()
        except RuntimeError as e:
            if "does not support TLS" in str(e):
                pytest.skip("Server does not support TLS")
            raise

        rows = await execute_raw(query, using=pool_db.name)

        if pool_db.backend == "postgres":
            assert rows[0]["ssl"] is True
        else:
            # MySQL: Ssl_cipher is non-empty when TLS active
            assert rows[0]["Value"] != ""

    @pytest.mark.asyncio
    async def test_pg_application_name(self, pool_db):
        if not pool_db.url.startswith("postgres"):
            pytest.skip("PostgreSQL-only setting")

        pool_db.settings = PoolSettings(pg_application_name="oxyde-test")
        await pool_db.connect()

        rows = await execute_raw(
            "SELECT application_name FROM pg_stat_activity "
            "WHERE pid = pg_backend_pid()",
            using=pool_db.name,
        )
        assert rows[0]["application_name"] == "oxyde-test"

    @pytest.mark.asyncio
    async def test_mysql_charset(self, pool_db):
        if not pool_db.url.startswith("mysql"):
            pytest.skip("MySQL-only setting")

        pool_db.settings = PoolSettings(mysql_charset="utf8mb4")
        await pool_db.connect()

        rows = await execute_raw(
            "SHOW VARIABLES LIKE 'character_set_client'",
            using=pool_db.name,
        )
        assert rows[0]["Value"] == "utf8mb4"

    @pytest.mark.asyncio
    async def test_mysql_collation(self, pool_db):
        if not pool_db.url.startswith("mysql"):
            pytest.skip("MySQL-only setting")

        pool_db.settings = PoolSettings(mysql_collation="utf8mb4_unicode_ci")
        await pool_db.connect()

        rows = await execute_raw(
            "SHOW VARIABLES LIKE 'collation_connection'",
            using=pool_db.name,
        )
        assert rows[0]["Value"] == "utf8mb4_unicode_ci"

    @pytest.mark.asyncio
    async def test_test_before_acquire(self, pool_db):
        pool_db.settings = PoolSettings(test_before_acquire=True)
        await pool_db.connect()

        # If test_before_acquire works, the pool pings before returning
        # connections. Verify the pool is functional.
        rows = await execute_raw("SELECT 1 AS ok", using=pool_db.name)
        assert rows[0]["ok"] == 1
