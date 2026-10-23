# Filtering

Oxyde supports Django-style field lookups for filtering queries.

## Basic Filtering

```python
# Exact match (default)
users = await User.objects.filter(status="active").all()

# Multiple conditions (AND)
users = await User.objects.filter(status="active", age__gte=18).all()
```

## Lookup Syntax

Lookups use double underscore notation: `field__lookup=value`

```python
filter(age__gte=18)      # age >= 18
filter(name__contains="john")  # name LIKE '%john%'
```

If no lookup is specified, `exact` is used:

```python
filter(status="active")        # status = 'active'
filter(status__exact="active") # same
```

## Comparison Lookups

| Lookup | SQL | Example |
|--------|-----|---------|
| `exact` | `=` | `filter(age=18)` |
| `gt` | `>` | `filter(age__gt=18)` |
| `gte` | `>=` | `filter(age__gte=18)` |
| `lt` | `<` | `filter(age__lt=65)` |
| `lte` | `<=` | `filter(age__lte=65)` |

```python
# Age between 18 and 65 (inclusive)
users = await User.objects.filter(age__gte=18, age__lte=65).all()
```

## Range Lookups

### between

```python
# BETWEEN 18 AND 65
users = await User.objects.filter(age__between=[18, 65]).all()
```

### range

Alias for `between`:

```python
users = await User.objects.filter(age__range=[18, 65]).all()
```

## String Lookups

| Lookup | SQL | Case Sensitive |
|--------|-----|----------------|
| `contains` | `LIKE '%...%'` | Yes |
| `icontains` | `ILIKE '%...%'` | No |
| `startswith` | `LIKE '...%'` | Yes |
| `istartswith` | `ILIKE '...%'` | No |
| `endswith` | `LIKE '%...'` | Yes |
| `iendswith` | `ILIKE '%...'` | No |
| `iexact` | `LOWER(...) = LOWER(...)` | No |

```python
# Contains (case-sensitive)
users = await User.objects.filter(name__contains="john").all()

# Contains (case-insensitive)
users = await User.objects.filter(name__icontains="john").all()

# Starts with
users = await User.objects.filter(email__startswith="admin@").all()

# Ends with
users = await User.objects.filter(email__endswith="@example.com").all()

# Exact (case-insensitive)
users = await User.objects.filter(email__iexact="JOHN@EXAMPLE.COM").all()
```

!!! note "Wildcard escaping"
    SQL wildcards (`%`, `_`, `\`) in filter values are automatically escaped as literal characters. For example, `filter(name__contains="50%")` matches the literal string `50%`, not "50 followed by anything". This applies to all LIKE-based lookups including `iexact`.

## NULL Checks

### isnull

```python
# IS NULL
users = await User.objects.filter(deleted_at__isnull=True).all()

# IS NOT NULL
users = await User.objects.filter(deleted_at__isnull=False).all()
```

## IN Lookup

```python
# IN clause
users = await User.objects.filter(status__in=["active", "pending"]).all()

# With subquery (manual)
admin_ids = await User.objects.filter(role="admin").values_list("id", flat=True).all()
posts = await Post.objects.filter(author_id__in=admin_ids).all()
```

## Date Lookups

### year / month / day

Extract date parts:

```python
# Posts from 2024
posts = await Post.objects.filter(created_at__year=2024).all()

# Posts from December 2024
posts = await Post.objects.filter(created_at__month=(2024, 12)).all()

# Posts from December 25, 2024
posts = await Post.objects.filter(created_at__day=(2024, 12, 25)).all()
```

## Array Lookups

Array fields (`list[T]`) and JSON fields holding arrays support three
lookups. Each takes a list or tuple:

| Lookup | Matches rows whose array... |
|---|---|
| `contains` | holds every given element |
| `contained_by` | has only elements from the given ones |
| `overlap` | shares at least one element with the given ones |

```python
class Article(Model):
    id: int | None = Field(default=None, db_pk=True)
    tags: list[str] = Field(default_factory=list)
    blocks: list[dict] = Field(default_factory=list)
    extra: dict | list | None = None  # JSON column

    class Meta:
        is_table = True

await Article.objects.filter(tags__contains=["rust"]).all()
await Article.objects.filter(tags__contained_by=("rust", "python", "go")).all()
await Article.objects.filter(tags__overlap=["rust", "python"]).all()

# Objects are compared as whole values
await Article.objects.filter(blocks__contains=[{"type": "code", "lang": "rust"}]).all()
```

How elements are compared:

- The array is treated as a set: order and duplicates do not matter.
- Objects match only as complete values (key order is ignored). `{"a": 1}`
  does not match an element `{"a": 1, "b": 2}`. Arrays nested inside
  elements keep their order.
- Strings are case-sensitive, booleans never equal numbers, and equal numbers
  match (`1` and `1.0`; a `Decimal` array stored as `1.50` matches `1.5`).
  MySQL compares non-integer JSON numbers as floating point, so values beyond
  double precision may compare equal there. In a `Decimal` array, an element
  that is not a number (written by another tool) never matches.
- `None` in the list matches a null element.
- An empty list: `contains=[]` matches every array, `overlap=[]` matches
  none, and `contained_by=[]` matches only empty arrays.
- A NULL column never matches, nor does its negation (SQL NULL semantics).
  A JSON value that is not an array (an object, a string, JSON `null`) does not
  match, so `exclude(...)` includes it. On SQLite, which does not validate JSON
  columns, neither does stored text that is not valid JSON, nor a BLOB;
  SQLite's binary JSONB is read as the JSON it holds.

Elements must fit the field's element type: `tags__contains=[1]` on a
`list[str]` field raises an error rather than silently matching nothing.
Values in a JSON operand are serialized the same way as when they are saved
(for example `UUID` and `Decimal` become strings).

`contains` with a string on a `str` field keeps its `LIKE` meaning, even when
the field is stored as JSON. Nested typed arrays (`list[list[int]]`) do not
support array lookups; store such data in a JSON field.

Backend notes:

- **PostgreSQL** uses the native `@>`, `<@` and `&&` operators for typed
  arrays, so GIN indexes on those columns apply. A list containing `None` is
  compared element by element instead, which scans rows. JSON(B) columns
  compare elements individually; `contains` checks `@>` first, which a GIN
  index on the column can serve, while `contained_by` and `overlap` scan rows.
- **MySQL** evaluates the lookups row by row with `JSON_TABLE` (MySQL 8.0.4+);
  multi-valued indexes are not used.
- **SQLite** evaluates the lookups row by row with `oxyde_array_match()`, a
  function Oxyde registers on its own connections. The database file stays
  plain SQLite, and other tools can read and write it as usual. Only the
  generated SQL needs Oxyde: SQL from `.sql()` that contains an array lookup
  does not run in the `sqlite3` shell or other tools. For the same reason,
  do not reference `oxyde_array_match()` in views, triggers or CHECK
  constraints.

## Q Expressions

For complex boolean logic (OR, NOT):

```python
from oxyde import Q

# OR
users = await User.objects.filter(
    Q(role="admin") | Q(role="moderator")
).all()

# NOT
users = await User.objects.filter(~Q(status="banned")).all()

# Complex
users = await User.objects.filter(
    Q(age__gte=18) & (Q(status="active") | Q(status="premium"))
).all()
```

### Combining Q Objects

| Operator | Meaning |
|----------|---------|
| `&` | AND |
| `\|` | OR |
| `~` | NOT |

```python
# (age >= 18 AND status = 'active') OR role = 'admin'
users = await User.objects.filter(
    (Q(age__gte=18) & Q(status="active")) | Q(role="admin")
).all()

# NOT (status = 'banned' OR status = 'suspended')
users = await User.objects.filter(
    ~(Q(status="banned") | Q(status="suspended"))
).all()
```

### Mixing Q and kwargs

```python
# Q expressions with keyword arguments
users = await User.objects.filter(
    Q(role="admin") | Q(role="moderator"),
    status="active"  # AND with the Q expression
).all()
```

## exclude()

Negate conditions:

```python
# NOT status = 'banned'
users = await User.objects.exclude(status="banned").all()

# Equivalent to
users = await User.objects.filter(~Q(status="banned")).all()
```

Chain with filter:

```python
# status = 'active' AND NOT role = 'bot'
users = await User.objects.filter(status="active").exclude(role="bot").all()
```

## Lookup Reference by Type

### String Fields

- `exact`, `iexact`
- `contains`, `icontains`
- `startswith`, `istartswith`
- `endswith`, `iendswith`
- `in`, `isnull`

### Numeric Fields (int, float, Decimal)

- `exact`
- `gt`, `gte`, `lt`, `lte`
- `between`, `range`
- `in`, `isnull`

### DateTime/Date Fields

- `exact`
- `gt`, `gte`, `lt`, `lte`
- `between`, `range`
- `year`, `month`, `day`
- `in`, `isnull`

### Boolean Fields

- `exact`
- `in`, `isnull`

### Array and JSON Fields

- `exact`
- `contains`, `contained_by`, `overlap` (see [Array Lookups](#array-lookups))
- `in`, `isnull`

### UUID Fields

- `exact`
- `gt`, `gte`, `lt`, `lte`
- `between`, `range` (inclusive)
- `in`, `isnull`

UUID comparisons follow the database's UUID ordering. PostgreSQL compares its
native UUID type; SQLite and MySQL compare the stored canonical UUID text.
Ordering does not necessarily reflect creation or commit time.

```python
# Fetch the next page in UUID order.
page = await (
    Event.objects.filter(id__gt=cursor)
    .order_by("id")
    .limit(50)
    .all()
)

# Include both endpoints.
events = await Event.objects.filter(id__range=(first_id, last_id)).all()
```

## Common Patterns

### Pagination

```python
page = 2
per_page = 20
users = await User.objects.filter(
    status="active"
).order_by("-created_at").offset((page - 1) * per_page).limit(per_page).all()
```

### Search

```python
query = "john"
users = await User.objects.filter(
    Q(name__icontains=query) | Q(email__icontains=query)
).all()
```

### Date Range

```python
from datetime import datetime, timedelta

now = datetime.utcnow()
last_week = now - timedelta(days=7)

recent = await Post.objects.filter(
    created_at__gte=last_week,
    created_at__lt=now
).all()
```

### Active/Soft Delete

```python
# Active records only
active = await User.objects.filter(deleted_at__isnull=True).all()

# Include deleted
all_users = await User.objects.all()
```

## Related Field Lookups (FK Traversal)

Oxyde supports Django-style filtering through foreign key relationships:

```python
# Filter posts by author's age (auto-joins author table)
posts = await Post.objects.filter(author__age__gte=18).all()

# Filter by nested FK
posts = await Post.objects.filter(author__company__name="Acme").all()

# Combine with Q expressions
posts = await Post.objects.filter(
    Q(author__age__gte=18) | Q(author__role="admin")
).all()
```

This automatically adds the necessary JOINs to the query.

### Related Fields Named Like Lookups

When the last part of a key is both a lookup name and a field on the related
model, a lookup the FK column itself supports wins; otherwise the key filters
the related field:

```python
# Author has fields named `gt` and `year`
filter(author__gt=5)        # author_id > 5 (FK columns support gt)
filter(author__year=2020)   # author.year = 2020 (FK columns have no year lookup)
filter(author__gt__exact=5) # author.gt = 5
```

## Known Limitations

### Ambiguous Columns with JOIN

When using `join()`, filter on FK column to avoid ambiguity:

```python
# May be ambiguous if both tables have 'id'
posts = await Post.objects.join("author").filter(id__in=[1,2,3]).all()

# Better: filter before join or use FK column
posts = await Post.objects.filter(id__in=[1,2,3]).join("author").all()
```

## Next Steps

- [Expressions](expressions.md) — F expressions for database operations
- [Aggregation](aggregation.md) — GROUP BY and aggregate functions
- [Queries](queries.md) — Full query reference
