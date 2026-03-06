from __future__ import annotations

import pytest

from oxyde import AsyncDatabase, F, Field, Model
from oxyde.models.registry import register_table


class TestArticleQueries:
    class Article(Model):
        id: int | None = Field(default=None, db_pk=True)
        title: str
        views: int

        class Meta:
            is_table = True
            table_name = "articles"

    @pytest.mark.asyncio
    async def test_fetch_models(self, sqlite_db: AsyncDatabase) -> None:
        articles = await self.Article.objects.all(using=sqlite_db.name)
        titles = [article.title for article in articles]
        assert titles == ["First", "Second", "Third"]

    @pytest.mark.asyncio
    async def test_lookup_and_order(self, sqlite_db: AsyncDatabase) -> None:
        articles = await self.Article.objects.filter(views__gte=10).all(
            using=sqlite_db.name
        )
        assert [article.title for article in articles] == ["First", "Third"]

        ordered = await self.Article.objects.filter().order_by("-views").all(client=sqlite_db)
        assert [article.title for article in ordered] == ["Third", "First", "Second"]

    @pytest.mark.asyncio
    async def test_values_list_flat(self, sqlite_db: AsyncDatabase) -> None:
        titles = await self.Article.objects.values_list("title", flat=True).all(client=sqlite_db)
        assert titles == ["First", "Second", "Third"]


class TestArticleManagerHelpers:
    class Article(Model):
        id: int | None = Field(default=None, db_pk=True)
        title: str
        views: int

        class Meta:
            is_table = True
            table_name = "articles"

    @pytest.mark.asyncio
    async def test_manager_shortcuts(self, sqlite_db: AsyncDatabase) -> None:
        article = await self.Article.objects.get(using=sqlite_db.name, id=1)
        assert article.title == "First"

        missing = await self.Article.objects.get_or_none(
            using=sqlite_db.name, title="Missing"
        )
        assert missing is None

        first = await self.Article.objects.first(using=sqlite_db.name)
        last = await self.Article.objects.last(using=sqlite_db.name)
        assert first.id == 1
        assert last.id == 3

        filtered_results = (
            await self.Article.objects.filter(views__gte=20)
            .limit(1)
            .all(using=sqlite_db.name)
        )
        assert filtered_results[0].id == 3

        count = await self.Article.objects.filter(views__gte=10).count(
            using=sqlite_db.name
        )
        assert count == 2


class TestArticleMutations:
    class Article(Model):
        id: int | None = Field(default=None, db_pk=True)
        title: str
        views: int

        class Meta:
            is_table = True
            table_name = "articles"

    @pytest.mark.asyncio
    async def test_create_and_update(self, sqlite_db: AsyncDatabase) -> None:
        article = await self.Article.objects.create(
            using=sqlite_db.name,
            title="Created",
            views=1,
        )
        assert article.title == "Created"

        created_count = await self.Article.objects.filter(title="Created").count(
            using=sqlite_db.name
        )
        assert created_count == 1

        updated = await self.Article.objects.filter(title="Created").update(
            views=99,
            using=sqlite_db.name,
        )
        assert updated == 1

        titles = await self.Article.objects.values_list("title", flat=True).all(client=sqlite_db)
        assert "Created" in titles

    @pytest.mark.asyncio
    async def test_delete(self, sqlite_db: AsyncDatabase) -> None:
        deleted = await self.Article.objects.filter(title="Second").delete(
            using=sqlite_db.name
        )
        assert deleted == 1

        remaining_query = self.Article.objects.values_list("title", flat=True)
        remaining = await remaining_query.all(client=sqlite_db)
        assert "Second" not in remaining

    @pytest.mark.asyncio
    async def test_bulk_create_and_expressions(self, sqlite_db: AsyncDatabase) -> None:
        # Delete all articles using filter without conditions
        await self.Article.objects.filter().delete(using=sqlite_db.name)

        new_rows = [
            {"title": "Bulk One", "views": 5},
            self.Article(title="Bulk Two", views=6),
        ]
        created = await self.Article.objects.bulk_create(new_rows, using=sqlite_db.name)

        assert [article.title for article in created] == ["Bulk One", "Bulk Two"]

        total = await self.Article.objects.count(using=sqlite_db.name)
        assert total == 2

        await self.Article.objects.filter(title="Bulk One").update(
            views=F("views") + 10,
            using=sqlite_db.name,
        )

        refreshed = await self.Article.objects.get(
            using=sqlite_db.name, title="Bulk One"
        )
        assert refreshed.views == 15


class TestRelationalQueries:
    class Author(Model):
        id: int | None = Field(default=None, db_pk=True)
        email: str
        name: str

        class Meta:
            is_table = True
            table_name = "authors"

    class Comment(Model):
        id: int | None = Field(default=None, db_pk=True)
        post_id: int = 0
        body: str = ""

        class Meta:
            is_table = True
            table_name = "comments"

    class Post(Model):
        id: int | None = Field(default=None, db_pk=True)
        title: str
        author: Author | None = None
        views: int
        comments: list[Comment] = Field(db_reverse_fk="post_id")

        class Meta:
            is_table = True
            table_name = "posts"

    @pytest.mark.asyncio
    async def test_join_and_prefetch(self, sqlite_db: AsyncDatabase) -> None:
        # Re-register models in case they were cleared by other tests
        register_table(self.Author, overwrite=True)
        register_table(self.Comment, overwrite=True)
        register_table(self.Post, overwrite=True)

        query = self.Post.objects.join("author").prefetch("comments").order_by("-views")
        posts = await query.all(client=sqlite_db)

        assert [post.title for post in posts] == [
            "Rust Patterns",
            "Kernel Notes",
            "Async ORM",
        ]

        by_title = {post.title: post for post in posts}

        rust = by_title["Rust Patterns"]
        assert rust.author is not None
        assert rust.author.email == "ada@example.com"
        assert sorted(comment.body for comment in rust.comments) == [
            "Great read!",
            "Thanks for sharing",
        ]

        kernel = by_title["Kernel Notes"]
        assert kernel.author is not None
        assert kernel.author.name == "Linus Torvalds"
        assert [comment.body for comment in kernel.comments] == ["Subscribed!"]

        async_post = by_title["Async ORM"]
        assert async_post.author is not None
        assert async_post.comments == []
