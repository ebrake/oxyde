"""Integration tests for edge cases, union, sql debug, and batch operations."""
from __future__ import annotations

import pytest

from oxyde import execute_raw

from .conftest import Author, Post, Tag, create_tag


class TestEmptyTable:
    @pytest.mark.asyncio
    async def test_empty_all(self, db):
        await execute_raw("DELETE FROM post_tags", using=db.name)
        await execute_raw("DELETE FROM tags", using=db.name)

        tags = await Tag.objects.all(using=db.name)
        assert tags == []

    @pytest.mark.asyncio
    async def test_empty_count(self, db):
        await execute_raw("DELETE FROM post_tags", using=db.name)
        await execute_raw("DELETE FROM tags", using=db.name)

        count = await Tag.objects.count(using=db.name)
        assert count == 0

    @pytest.mark.asyncio
    async def test_empty_exists(self, db):
        await execute_raw("DELETE FROM post_tags", using=db.name)
        await execute_raw("DELETE FROM tags", using=db.name)

        exists = await Tag.objects.exists(using=db.name)
        assert exists is False

    @pytest.mark.asyncio
    async def test_empty_first(self, db):
        await execute_raw("DELETE FROM post_tags", using=db.name)
        await execute_raw("DELETE FROM tags", using=db.name)

        first = await Tag.objects.order_by("id").first(using=db.name)
        assert first is None


class TestUnicodeData:
    @pytest.mark.asyncio
    async def test_unicode_roundtrip(self, db):
        author = await Author.objects.create(
            name="Юникод Тест 日本語 🦀",
            email="unicode@test.com",
            using=db.name,
        )
        fetched = await Author.objects.get(id=author.id, using=db.name)
        assert fetched.name == "Юникод Тест 日本語 🦀"


class TestLargeBatch:
    @pytest.mark.asyncio
    async def test_bulk_create_with_batch_size(self, db):
        tags = [Tag(name=f"batch_tag_{i}") for i in range(50)]
        created = await Tag.objects.bulk_create(
            tags, batch_size=10, using=db.name
        )
        assert len(created) == 50

        count = await Tag.objects.count(using=db.name)
        assert count == 53  # 3 seed + 50 new


class TestUnion:
    @pytest.mark.asyncio
    async def test_union(self, db):
        """UNION removes duplicates."""
        q1 = Post.objects.filter(author_id=1)  # posts 1, 2
        q2 = Post.objects.filter(published=True)  # posts 1, 2, 3, 5

        posts = await q1.union(q2).order_by("id").all(using=db.name)
        ids = [p.id for p in posts]
        # Union of {1,2} and {1,2,3,5} = {1,2,3,5}
        assert ids == [1, 2, 3, 5]

    @pytest.mark.asyncio
    async def test_union_all(self, db):
        """UNION ALL keeps duplicates."""
        q1 = Post.objects.filter(author_id=1)  # posts 1, 2
        q2 = Post.objects.filter(id__in=[1, 3])  # posts 1, 3

        posts = await q1.union_all(q2).order_by("id").all(using=db.name)
        ids = [p.id for p in posts]
        # Union all: [1,2] + [1,3] = [1,1,2,3]
        assert ids == [1, 1, 2, 3]


class TestSqlDebug:
    def test_sql_returns_query(self):
        """sql() returns (sql_string, params) without DB connection."""
        sql, params = Post.objects.filter(published=True, views__gt=100).sql(
            dialect="sqlite"
        )
        assert isinstance(sql, str)
        assert isinstance(params, list)
        assert "posts" in sql.lower()
        assert len(params) == 2
