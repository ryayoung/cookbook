# Polars Style and Syntax

```python
import polars as pl
from polars import (
    col as c,
    selectors as cs,
)
```

---

Here is the same code, written twice. The two *really are* identical.
They use the **same functions** to create the **same expressions**
to send the **same query** to the **same engine**.

**Version A**

```python
.sort( [ 'period', pl.col('stuff').cast(pl.Int64) ] )
.with_columns(
    [
        ( ( pl.col("current") - pl.col("prior") ) / pl.col("prior") ).alias("pct_change"),
        ( pl.col("sum_stuff") / pl.col('count_stuff') ).alias("avg_stuff"),
    ]
)
```

**Version B**

```python
.sort(c.period, c.stuff.cast(int))
.with_columns(
    pct_change=(c.current - c.prior) / c.prior,
    avg_stuff=(c.sum_stuff / c.count_stuff),
)
```

---

Another example. **Same code**, **same functions**, **same expressions**, written twice.

**A**

```python
.group_by(
    [
        pl.col("first_name").str.to_lowercase().alias("first_name"),
        pl.col("country_code").str.to_uppercase().alias("country"),
    ]
)
.agg(
    [
        pl.col("sum_stuff").sum().alias("sum_stuff"),
        pl.col("count_stuff").sum().alias("count_things"),
    ]
)
```

**B**

```python
.group_by(
    c.first_name.str.to_lowercase(),
    country=c.country_code.str.to_uppercase(),
)
.agg(
    c.sum_stuff.sum(),
    count_things=c.count_stuff.sum(),
)
```

---

Another. Five versions of the same code:

```python
.agg(
    [ pl.col('measure_foo').mean(), pl.col('measure_bar').mean(), pl.col('measure_baz').mean() ]
)
```

```python
.agg(
    c.measure_foo.mean(), c.measure_bar.mean(), c.measure_baz.mean(),
)
```

```python
.agg(
    c('measure_foo', 'measure_bar', 'measure_baz').mean(),
)
```

```python
.agg(
    cs.starts_with('measure_').mean(),
)
```

```python
.agg(
    cs.numeric().mean(),
)
```

---

Relevant if you're coming from Pandas:

```python
.filter(
    ( c.country == 'US' )
    & ( c.size > 500 )
    & ( c.year_created > 2020 )
)
```

```python
.filter(
    c.country == 'US',
    c.size > 500,
    c.year_created > 2020,
)
```

---

Treat Polars like a language: Pick standards, and stick to them.


## Please. Don't use `pl.` for *everything*

This isn't pandas.

In pandas, most things are accessed as methods of your dataframe, whereas `pd.` is rare.

In Polars, it's the opposite. You'll *almost never* reference your dataframe.

In pandas, your project might use 12 different `df.` methods, 7 `pd.` functions, and
6 keyword parameters, to get the job done.

In Polars, the same project might use only 3 methods and 3 functions...

...**but you'll reuse those same 3 `pl.` functions everywhere, for everything, all the time**.

Ergonomics matters.


### Direct Imports

There are a few things in the `pl.` namespace which, when needed,
you'll want to import *directly*, instead of accessing from `pl.`. You'll thank yourself later.

1. `col as c`
2. `selectors as cs` ("cs" means "Column Selectors")
3. `lit`
4. `when`
5. `struct`

Decide what that list should be, and use `pl.` for everything else.


#### Be consistent

Decide, in advance, what those direct imports should be.

Keep it minimal.

Stick to the pattern. Don't let each module decide on its own.

---


## Like SQL, Polars is declarative and lazy

**Which means**: You aren't responsible for (and should avoid) optimizing your code
on a line-by-line basis.

How you *write* something doesn't directly translate to how it will *execute*.

---

1. **Obviously-unnecessary work has no performance cost.**

   If you do redundant or unnecessary work, or compute columns but never
   end up *using* them, then the engine will **skip** that work. Those expressions
   won't run. (And, if your input is Parquet, unused columns and filtered-out rows
   won't even be read from the file. It'll be like they never existed.)

   Suppose you have 3 string columns: firstname, middlename, lastname.

   **Version A**

   ```python
   .filter(
       c.firstname.str.starts_with('A'),
       c.middlename.str.starts_with('B'),
       c.lastname.str.starts_with('C'),
   )
   ```

   **Version B**

   ```python
   .filter(c.firstname.cast(str).cast(str).cast(str).str.starts_with('A'))
   .filter(c.middlename.cast(str).cast(str).cast(str).str.starts_with('B'))
   .filter(c.lastname.cast(str).cast(str).cast(str).str.starts_with('C'))
   ```

   There should be no performance difference between
   these queries. The engine may reduce both to identical query plans.

2. **Assume the engine's optimizer is smarter than you.**

   Rule of thumb: Don't bother optimizing anything. Find the most
   ergonomic, intuitive, and readable way to write something, and
   do it that way.


## Native Data Types

In Polars, all data structures are implemented from scratch, in Rust.

That includes **strings**, **nulls**, **lists** (variable-length arrays),
**structs**, and **nested lists/arrays/structs**. All of which are concepts
that fundamentally cannot and will never exist in *numerical* engines like
Numpy. (Variable-length data would break Numpy's whole reality.)

There are no "slow" data types.

---

*Technically* it is possible to store Python objects in a dataframe, like Pandas
does. There might be good use cases for this, but I have yet to encounter one.


## Immutability

There's no such thing as **'inplace'** operations in Polars, or **copying** a
dataframe.

Data isn't just immutable. It's **deeply** immutable.


## `DataFrame` vs. `LazyFrame`

Polars is built entirely around `LazyFrame`: An immutable structure,
representing a *plan* that will execute when `.collect()` is called.

`DataFrame` is a convenience feature: A wrapper around `LazyFrame`
that calls `.collect()` for you automatically, after everything you do,
making most optimizations impossible.

They have (mostly) identical APIs/methods. If you encounter a method
that only exists on `DataFrame` (such as `.pivot()`), that's usually a good
sign that you probably **should not** use that method. For example,
`DataFrame.iter_rows()`. Bad idea. Don't touch it. That's what `when().then().otherwise()`
is for.


## Is there anything performance-related I *should* think about?

Yes.

**Data Types**.

The limiting factor will always be **memory**. Not CPU time.
Queries will take single-digit-milliseconds, right up until the data gets
too large for memory. That's where things begin to slow down.

You can often **reduce the size of a dataframe by 10x**, just by changing
the data types.

---

Rule of thumb: Use the *smallest* i.e. most *restrictive* type possible
for a given column.

1. **Text data**

   You've got three options. Prefer them, in descending order (`Enum` is best).

   1. `pl.Enum([...])`
      - You know the exact categories.
      - Under the hood: Column is stored as the smallest **UInt** possible, for
        the number of values you provided. So that usually means `UInt8`, unless
        you provided more than 255-ish values.
   2. `pl.Categorical()`
      - You don't know the exact categories, but the values *are* some discrete set
        of values.
   3. `pl.String` (i.e. `str`)
      - Use only for arbitrary text. e.g. `description`, `first_name`,
        `title`, `chat_uuid`, `comments`, `message_content`, etc.

2. **Integers**

   The default is `Int64`: A *signed*, 64bit int.
   In schemas and `.cast()` expressions, Python `int` is shorthand for `pl.Int64`.

   - **Signed**: `Int8`, `Int16`, `Int32`, `Int64`, `Int128`
   - **Unsigned**: `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`

   Google the number of possible values for each of these. Memorize those limits as
   best as you can. Look them up as needed until you get the hang of it.

   Decide if your values are *signed* or *unsigned*. Then pick the smallest type that
   you consider 'correct'.
