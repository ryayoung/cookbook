# Working with nested data in Polars

## Imports

```python
# For less-commonly-used things
import polars as pl
# Always-very-common Expr entrypoints
from polars import (
    # Selecting columns
    col as c,
    selectors as cs,
    # Creating nested data structures and literal values
    lit,
    struct,
    concat_arr as array,
    # Other core keywords
    when,
)
```

## **Please**

Here's the same code, written twice.

The two queries *really are* identical.
They do the same thing, in the same way: They create the same expressions
using the same API to send the same query to the same engine.

**Same code, V1**

```python
.sort( [ 'period', pl.col('stuff').cast(pl.Int64) ] )
.with_columns(
    [
        ( ( pl.col("current") - pl.col("prior") ) / pl.col("prior") ).alias("pct_change"),
        ( pl.col("sum_stuff") / pl.col('count_stuff') ).alias("avg_stuff"),
    ]
)
```

**Same code, V2**

```python
.sort(c.period, c.stuff.cast(int))
.with_columns(
    pct_change=(c.current - c.prior) / c.prior,
    avg_stuff=(c.sum_stuff / c.count_stuff),
)
```

Languages have formatting rules and standards. Treat Polars like a language.
Pick standards, and stick to them.

### What should you do?

Stick to 'V2'. Use 'V1' features *when-needed*.
Prioritize ergonomics, because you'll be repeating the same pattern tens of thousands of times.

For example, instead of `.sort("period").drop("stuff")`, prefer
`.sort(c.period).drop("stuff")`, because `.sort` takes expressions.

---

## Arrow

An engine that uses the [Arrow](https://arrow.apache.org/docs/dev/format/Columnar.html)
approach for its memory layout
(like Polars) will let you work with nested object/array data **without**
the memory and compute cost of those objects and arrays existing
in your program. Because they don't exist.

Which means: If you have a *hand-rolled* algorithm involving
a long sequence of deeply-nested objects, you could rewrite all the
data structures in C or Assembly and **still** be 10x less memory efficient
than an equivalent program that uses Arrow's layout strategy.

Where yours might allocate 500 million C structs and kill a large VM,
an equivalent program with an Arrow-inspired layout might allocate 1 struct
and run on a laptop.

---

In Polars, these two dataframes have near-identical memory footprints
at scale (i.e. assuming you have more than this example's 4 rows):

**1**

```json
[
  {
    "point_a_x": 1,
    "point_a_y": 5,
    "point_b_x": 9,
    "point_b_y": 13
  },
  {
    "point_a_x": 2,
    "point_a_y": 6,
    "point_b_x": 10,
    "point_b_y": 14
  },
  {
    "point_a_x": 3,
    "point_a_y": 7,
    "point_b_x": 11,
    "point_b_y": 15
  },
  ...other rows
]
```

**2**

```json
[
  {
    "data": {
      "line": {
        "point_a": {
          "x": 1,
          "y": 5
        },
        "point_b": {
          "x": 9,
          "y": 13
        }
      }
    }
  },
  {
    "data": {
      "line": {
        "point_a": {
          "x": 2,
          "y": 6
        },
        "point_b": {
          "x": 10,
          "y": 14
        }
      }
    }
  },
  {
    "data": {
      "line": {
        "point_a": {
          "x": 3,
          "y": 7
        },
        "point_b": {
          "x": 11,
          "y": 15
        }
      }
    }
  },
  ...other rows
]
```

---

Let's start with this dataframe of 2 rows.

```python
df = pl.DataFrame({
    'type': ['Movie', 'Movie'],
    'rating': ['TV-Y', 'TV-14'],
    'country': ['United States', 'Japan'],
    'runtime': [3, 5],
})
```

```
shape: (2, 4)
┌───────┬────────┬───────────────┬─────────┐
│ type  ┆ rating ┆ country       ┆ runtime │
│ ---   ┆ ---    ┆ ---           ┆ ---     │
│ str   ┆ str    ┆ str           ┆ i64     │
╞═══════╪════════╪═══════════════╪═════════╡
│ Movie ┆ TV-Y   ┆ United States ┆ 3       │
│ Movie ┆ TV-14  ┆ Japan         ┆ 5       │
└───────┴────────┴───────────────┴─────────┘
```

We will use JSON outputs when printing dataframes.
(Call `.to_dicts()` on each result.)

```python
df.select(
    c.type,
    c.rating,
    c.country,
    c.runtime,
)
```

```
[
  {
    "type": "Movie",
    "rating": "TV-Y",
    "country": "United States",
    "runtime": 3
  },
  {
    "type": "Movie",
    "rating": "TV-14",
    "country": "Japan",
    "runtime": 5
  }
]
```


# Structs

In a Struct column, each value *represents* an object with fixed fields
and data types.

If you had a `name` column with values...

```js
{ first: "John", last: "Smith" },
{ first: "Jane", last: "Doe" },
```

...`name`'s dtype is `Struct({"first": String, "last": String})`.

> [!IMPORTANT]
> This has **near-zero overhead** compared to separate `first_name`/`last_name` columns,
> because no `name` objects actually exist. Instead, *basically*, the column itself has
> columns. And each column *doesn't* store a sequence of name strings. It stores *a string*.
> For the entire column. That's Arrow, in a nutshell.

Let's combine our 4 columns into 1 struct column.

```python
df.select(
    DATA=struct(
        c.type,
        c.rating,
        c.country,
        c.runtime,
    ),
)
```

```
[
  {
    "DATA": {
      "type": "Movie",
      "rating": "TV-Y",
      "country": "United States",
      "runtime": 3
    }
  },
  {
    "DATA": {
      "type": "Movie",
      "rating": "TV-14",
      "country": "Japan",
      "runtime": 5
    }
  }
]
```

or nest it even more...

```python
df.select(
    DATA_A1=struct(
        c.type,
    ),
    DATA_B1=struct(
        c.rating,
        DATA_B2=struct(
            c.country,
            DATA_B3=struct(
                c.runtime,
            ),
        ),
    ),
)
```

```
[
  {
    "DATA_A1": {
      "type": "Movie"
    },
    "DATA_B1": {
      "rating": "TV-Y",
      "DATA_B2": {
        "country": "United States",
        "DATA_B3": {
          "runtime": 3
        }
      }
    }
  },
  {
    "DATA_A1": {
      "type": "Movie"
    },
    "DATA_B1": {
      "rating": "TV-14",
      "DATA_B2": {
        "country": "Japan",
        "DATA_B3": {
          "runtime": 5
        }
      }
    }
  }
]
```

We can reverse this with `.unnest()`:

```python
(
    <previous_select_query>
    .unnest("DATA_A1", "DATA_B1")
    .unnest("DATA_B2")
    .unnest("DATA_B3")
)
```

```
[
  {
    "type": "Movie",
    "rating": "TV-Y",
    "country": "United States",
    "runtime": 3
  },
  {
    "type": "Movie",
    "rating": "TV-14",
    "country": "Japan",
    "runtime": 5
  }
]
```

# Arrays

Unlike lists, arrays are fixed-length.

In how you'll *use* them, arrays are more like structs than lists.

Same query from earlier, but with `array` instead of `struct`.

```python
df.select(
    DATA=array(  # resulting dtype: `array[str, 3]`
        c.type,
        c.rating,
        c.country,
    )
)
```

```
[
  {
    "DATA": ["Movie", "TV-Y", "United States"]
  },
  {
    "DATA": ["Movie", "TV-14", "Japan"]
  }
]
```

Putting them together:

```python
df.select(
    dimensions=array(
        struct(id=lit("show.type"), value=c.type),
        struct(id=lit("show.rating"), value=c.rating),
        struct(id=lit("show.country"), value=c.country),
    ),
    metrics=array(
        struct(id=lit("show.runtime"), value=c.runtime),
    )
)
```

```
[
  {
    "dimensions": [
      { "id": "show.type", "value": "Movie" },
      { "id": "show.rating", "value": "TV-Y" },
      { "id": "show.country", "value": "United States" }
    ],
    "metrics": [
      { "id": "show.runtime", "value": 3 }
    ]
  },
  {
    "dimensions": [
      { "id": "show.type", "value": "Movie" },
      { "id": "show.rating", "value": "TV-14" },
      { "id": "show.country", "value": "Japan" }
    ],
    "metrics": [
      { "id": "show.runtime", "value": 5 }
    ]
  }
]
```

# Lists

Key insight: **A list is an aggregation. The simplest kind.**

## In a group-by/agg, `.agg()` will take any kind of expression

You **don't** have to pass an aggregation function.

Inside an `.agg()` context, each of your expressions is evaluated
against a *group* to produce a value.

If don't aggregate the group, you'll get the group itself back.
That list will be your value.

```python
df = pl.DataFrame({'show_type': ['Movie', 'Movie'], 'runtime': [150, 100]})

print(
    df  # Original data
)
print(
    df.group_by(c.show_type)
    .agg(
        c.runtime.sum(),  # Given a group, get the runtimes, and sum them
    )
)
print(
    df.group_by(c.show_type)
    .agg(
        c.runtime,        # Given a group, get the runtimes.
    )
)
```

```
┌───────────┬─────────┐
│ show_type ┆ runtime │
│ str       ┆ i64     │
╞═══════════╪═════════╡
│ Movie     ┆ 150     │
│ Movie     ┆ 100     │
└───────────┴─────────┘
┌───────────┬─────────┐
│ show_type ┆ runtime │
│ str       ┆ i64     │
╞═══════════╪═════════╡
│ Movie     ┆ 250     │     # c.runtime.sum()
└───────────┴─────────┘
┌───────────┬────────────┐
│ show_type ┆ runtime    │
│ str       ┆ list[i64]  │
╞═══════════╪════════════╡
│ Movie     ┆ [150, 100] │  # c.runtime
└───────────┴────────────┘
```

# Building deep, hierarchical trees

In the following examples, we will use **only** `group_by()` and `agg()`
to transform this data...

```
┌───────────┬────────┬─────────┬─────────┐
│ show_type ┆ rating ┆ show_id ┆ runtime │
╞═══════════╪════════╪═════════╪═════════╡
│ Movie     ┆ G      ┆ show1   ┆ 150     │
│ Movie     ┆ G      ┆ show2   ┆ 100     │
│ Movie     ┆ PG     ┆ show3   ┆ 100     │
│ Movie     ┆ PG     ┆ show4   ┆ 100     │
│ TV Show   ┆ TV-G   ┆ show5   ┆ 1500    │
│ TV Show   ┆ TV-G   ┆ show6   ┆ 1000    │
│ TV Show   ┆ TV-PG  ┆ show7   ┆ 1000    │
│ TV Show   ┆ TV-PG  ┆ show8   ┆ 1000    │
└───────────┴────────┴─────────┴─────────┘
```

...into the following tree object:

```json
{
  "entity": "show",          # Root
  "children": [
    {
      "show_type": "Movie",  # Root -> show_type='Movie'
      "children": [
        {
          "rating": "G",     # Root -> show_type='Movie' -> rating='G'
          "children": [
            { "show_id": "show1", "runtime": 150 },
            { "show_id": "show2", "runtime": 100 }
          ]
        },
        {
          "rating": "PG",
          "children": [
            { "show_id": "show3", "runtime": 100 },
            { "show_id": "show4", "runtime": 100 }
          ]
        }
      ]
    },
    {
      "show_type": "TV Show",
      "children": [
        {
          "rating": "TV-PG",
          "children": [
            { "show_id": "show7", "runtime": 1000 },
            { "show_id": "show8", "runtime": 1000 }
          ]
        },
        {
          "rating": "TV-G",
          "children": [
            { "show_id": "show5", "runtime": 1500 },
            { "show_id": "show6", "runtime": 1000 }
          ]
        }
      ]
    }
  ]
}
```

> [!NOTE]
> This kind of tree makes a nice payload for a UI to render a hierarchical
> pivot table, where users can click into each row to see values broken down
> by another dimension.


### Starting dataframe

```python
df = pl.DataFrame(
    [
        ['Movie', 'G', 'show1', 150],
        ['Movie', 'G', 'show2', 100],
        ['Movie', 'PG', 'show3', 100],
        ['Movie', 'PG', 'show4', 100],
        ['TV Show', 'TV-G', 'show5', 1500],
        ['TV Show', 'TV-G', 'show6', 1000],
        ['TV Show', 'TV-PG', 'show7', 1000],
        ['TV Show', 'TV-PG', 'show8', 1000],
    ],
    schema=['show_type', 'rating', 'show_id', 'runtime'],
    orient='row',
)
```

```
[
  { "show_type": "Movie", "rating": "G", "show_id": "show1", "runtime": 150 },
  { "show_type": "Movie", "rating": "G", "show_id": "show2", "runtime": 100 },
  { "show_type": "Movie", "rating": "PG", "show_id": "show3", "runtime": 100 },
  { "show_type": "Movie", "rating": "PG", "show_id": "show4", "runtime": 100 },
  { "show_type": "TV Show", "rating": "TV-G", "show_id": "show5", "runtime": 1500 },
  { "show_type": "TV Show", "rating": "TV-G", "show_id": "show6", "runtime": 1000 },
  { "show_type": "TV Show", "rating": "TV-PG", "show_id": "show7", "runtime": 1000 },
  { "show_type": "TV Show", "rating": "TV-PG", "show_id": "show8", "runtime": 1000 }
]
```


### Building the JSON Tree

*How do I make data so deeply nested? Do I put one `agg()` inside another?*

Nope.

**The trick**: Apply _multiple_ `group_by().agg()` transformations, sequentially, for each
list-nesting level. Start with the *deepest*, and work your way *back up* to the root.

*Level 1* (leaves): Aggregate the `show_id`-level (leaf) nodes - `struct(show_id, runtime)` - as
a `children` list under each `show_type` + `rating` combination.

```python
df.group_by(
    c.show_type,
    c.rating,
).agg(
    children=struct(
        c.show_id,
        c.runtime,
    ),
)
```

```json
[
  {
    "show_type": "Movie",
    "rating": "PG",
    "children": [
      { "show_id": "show3", "runtime": 100 },
      { "show_id": "show4", "runtime": 100 }
    ]
  },
  {
    "show_type": "Movie",
    "rating": "G",
    "children": [
      { "show_id": "show1", "runtime": 150 },
      { "show_id": "show2", "runtime": 100 }
    ]
  },
  {
    "show_type": "TV Show",
    "rating": "TV-G",
    "children": [
      { "show_id": "show5", "runtime": 1500 },
      { "show_id": "show6", "runtime": 1000 }
    ]
  },
  {
    "show_type": "TV Show",
    "rating": "TV-PG",
    "children": [
      { "show_id": "show7", "runtime": 1000 },
      { "show_id": "show8", "runtime": 1000 }
    ]
  }
]
```

*Level 2*: Aggregate the `rating`-level nodes - `struct(rating, children)` - as a `children`
list under each `show_type`.

```python
<previous_code>
.group_by(
    c.show_type,
)
.agg(
    children=struct(
        c.rating,
        c.children,
    ),
)
```

```json
[
  {
    "show_type": "Movie",
    "children": [
      {
        "rating": "PG",
        "children": [
          { "show_id": "show3", "runtime": 100 },
          { "show_id": "show4", "runtime": 100 }
        ]
      },
      {
        "rating": "G",
        "children": [
          { "show_id": "show1", "runtime": 150 },
          { "show_id": "show2", "runtime": 100 }
        ]
      }
    ]
  },
  {
    "show_type": "TV Show",
    "children": [
      {
        "rating": "TV-PG",
        "children": [
          { "show_id": "show7", "runtime": 1000 },
          { "show_id": "show8", "runtime": 1000 }
        ]
      },
      {
        "rating": "TV-G",
        "children": [
          { "show_id": "show5", "runtime": 1500 },
          { "show_id": "show6", "runtime": 1000 }
        ]
      }
    ]
  }
]
```

*Level 3* (root): Same as the prior step, but we need to cheat and make
a constant column, `entity=lit('show')` to serve as the root node in our tree,
so we can aggregate the `show_type`s into a single `children` list.

```python
<previous_code>
.group_by(
    entity=lit('show'),
)
.agg(
    children=struct(
        c.show_type,
        c.children,
    ),
).to_dicts()[0]
```

which gives us our final result.

Here's the full query:

```python
(
    df
    # Level 1: [ { show_id, runtime }, ...]
    .group_by(
        c.show_type,
        c.rating,
    ).agg(
        children=struct(
            c.show_id,
            c.runtime,
        ),
    )
    # Level 2: [ { rating, children }, ...]
    .group_by(
        c.show_type,
    )
    .agg(
        children=struct(
            c.rating,
            c.children,
        ),
    )
    # Level 3: [ { show_type, children }, ...]
    .group_by(
        entity=lit('show'),  # (constant column, for the root)
    )
    .agg(
        children=struct(
            c.show_type,
            c.children,
        ),
    )
    # Extract root (row 0): { entity, children }
    .to_dicts()[0]
)
```

```json
{
  "entity": "show",
  "children": [
    {
      "show_type": "Movie",
      "children": [
        {
          "rating": "PG",
          "children": [
            { "show_id": "show3", "runtime": 100 },
            { "show_id": "show4", "runtime": 100 }
          ]
        },
        {
          "rating": "G",
          "children": [
            { "show_id": "show1", "runtime": 150 },
            { "show_id": "show2", "runtime": 100 }
          ]
        }
      ]
    },
    {
      "show_type": "TV Show",
      "children": [
        {
          "rating": "TV-G",
          "children": [
            { "show_id": "show5", "runtime": 1500 },
            { "show_id": "show6", "runtime": 1000 }
          ]
        },
        {
          "rating": "TV-PG",
          "children": [
            { "show_id": "show7", "runtime": 1000 },
            { "show_id": "show8", "runtime": 1000 }
          ]
        }
      ]
    }
  ]
}
```


### Guess what

Everything we just did was **non-destructive and easily reversible**.

Watch:

```python
(
    <previous_completed_query>
    .explode('children').unnest('children')
    .explode('children').unnest('children')
    .explode('children').unnest('children')
    .drop('entity')
)
```

```json
[
  { "show_type": "Movie", "rating": "PG", "show_id": "show3", "runtime": 100 },
  { "show_type": "Movie", "rating": "PG", "show_id": "show4", "runtime": 100 },
  { "show_type": "Movie", "rating": "G", "show_id": "show1", "runtime": 150 },
  { "show_type": "Movie", "rating": "G", "show_id": "show2", "runtime": 100 },
  { "show_type": "TV Show", "rating": "TV-G", "show_id": "show5", "runtime": 1500 },
  { "show_type": "TV Show", "rating": "TV-G", "show_id": "show6", "runtime": 1000 },
  { "show_type": "TV Show", "rating": "TV-PG", "show_id": "show7", "runtime": 1000 },
  { "show_type": "TV Show", "rating": "TV-PG", "show_id": "show8", "runtime": 1000 }
]
```

# Real production-ready report tree

Using the fundamentals learned so far, let's make a "legit" report tree
for a UI to render as a fancy nested hierarchy table with all the bells
and whistles. Even sparklines! (mini inline charts).

We will take this dataset of all 8700 netflix movies and tv shows...

```
shape: (8_790, 5)
┌─────────────┬──────────┬────────────┬───────────────┬─────────────────┐
│ dim_show_id ┆ dim_type ┆ dim_rating ┆ dim_country   ┆ measure_runtime │
│ ---         ┆ ---      ┆ ---        ┆ ---           ┆ ---             │
│ str         ┆ cat      ┆ cat        ┆ cat           ┆ i64             │
╞═════════════╪══════════╪════════════╪═══════════════╪═════════════════╡
│ s4874       ┆ Movie    ┆ G          ┆ Canada        ┆ 87              │
│ s5737       ┆ Movie    ┆ G          ┆ Canada        ┆ 92              │
│ s7636       ┆ Movie    ┆ G          ┆ France        ┆ 84              │
│ s3473       ┆ Movie    ┆ G          ┆ Germany       ┆ 79              │
│ s6117       ┆ Movie    ┆ G          ┆ Ireland       ┆ 85              │
│ …           ┆ …        ┆ …          ┆ …             ┆ …               │
│ s5064       ┆ TV Show  ┆ TV-Y7      ┆ United States ┆ 2418            │
│ s3944       ┆ TV Show  ┆ TV-Y7      ┆ United States ┆ 3224            │
│ s3248       ┆ TV Show  ┆ TV-Y7      ┆ United States ┆ 3224            │
│ s4312       ┆ TV Show  ┆ TV-Y7      ┆ United States ┆ 3224            │
│ s7647       ┆ TV Show  ┆ TV-Y7-FV   ┆ Canada        ┆ 806             │
└─────────────┴──────────┴────────────┴───────────────┴─────────────────┘
```

...and build a nested JSON report that can go straight to UI components
for rendering, without any post-processing/assembling of the tree.

Do it end-to-end, in a single query.

## End Result

(Note: The `metrics[].values` field is mainly used for inline charts, e.g. sparklines.)

```json
{
  "root": {
    "key": { "kind": "root_total", "ref_id": "fact:show", "value": null, "label": null },
    "metrics": [
      { "metric_id": "metric.sum:show.runtime", "value": "2490858", "values": null },
      { "metric_id": "metric.median:show.runtime", "value": "111.0", "values": null },
      { "metric_id": "metric.mean:show.runtime", "value": "283.37", "values": null }
    ],
    "breakdown": {
      "by": { "how": "dimension_values", "dim_id": "dim:show.type" },
      "limit": null,
      "children": [
        {
          "key": { "kind": "dimension_value", "ref_id": "dim:show.type", "value": "TV Show", "label": null },
          "metrics": [
            { "metric_id": "metric.sum:show.runtime", "value": "1880801", "values": null },
            { "metric_id": "metric.median:show.runtime", "value": "403.0", "values": null },
            { "metric_id": "metric.mean:show.runtime", "value": "706.01", "values": null }
          ],
          "breakdown": {
            "by": { "how": "dimension_values", "dim_id": "dim:show.rating" },
            "limit": {
              "type": "metric_top_n",
              "from_total_n": 9,
              "kept_n": 2,
              "with_values": "highest",
              "metric_id": "metric.sum:show.runtime"
            },
            "children": [
              {
                "key": { "kind": "dimension_value", "ref_id": "dim:show.rating", "value": "TV-MA", "label": null },
                "metrics": [
                  { "metric_id": "metric.sum:show.runtime", "value": "775372", "values": null },
                  { "metric_id": "metric.median:show.runtime", "value": "403.0", "values": null },
                  { "metric_id": "metric.mean:show.runtime", "value": "678.37", "values": null }
                ],
                "breakdown": {
                  "by": { "how": "dimension_values", "dim_id": "dim:show.country" },
                  "limit": {
                    "type": "metric_top_n",
                    "from_total_n": 52,
                    "kept_n": 3,
                    "with_values": "highest",
                    "metric_id": "metric.sum:show.runtime"
                  },
                  "children": [
                    {
                      "key": { "kind": "dimension_value", "ref_id": "dim:show.country", "value": "United States", "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "281294", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "403.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "820.1", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["403", "806", "1209", "1612", "2015", "2418", "2821", "3224", "3627", "4030"]
                        }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": {
                        "kind": "dimension_value",
                        "ref_id": "dim:show.country",
                        "value": "United Kingdom",
                        "label": null
                      },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "85839", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "403.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "809.8", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["403", "806", "1209", "1612", "2015", "3627"]
                        }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": { "kind": "dimension_value", "ref_id": "dim:show.country", "value": "Pakistan", "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "61659", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "403.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "527.0", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["403", "806", "1209", "2821", "3224", "3627"]
                        }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": { "kind": "total_other", "ref_id": "dim:show.country", "value": null, "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "346580", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "403.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "600.66", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["403", "806", "1209", "1612", "2015", "2418", "2821", "3224", "4836"]
                        }
                      ],
                      "breakdown": null
                    }
                  ]
                }
              },
              {
                "key": { "kind": "dimension_value", "ref_id": "dim:show.rating", "value": "TV-14", "label": null },
                "metrics": [
                  { "metric_id": "metric.sum:show.runtime", "value": "531960", "values": null },
                  { "metric_id": "metric.median:show.runtime", "value": "403.0", "values": null },
                  { "metric_id": "metric.mean:show.runtime", "value": "728.71", "values": null }
                ],
                "breakdown": {
                  "by": { "how": "dimension_values", "dim_id": "dim:show.country" },
                  "limit": {
                    "type": "metric_top_n",
                    "from_total_n": 39,
                    "kept_n": 3,
                    "with_values": "highest",
                    "metric_id": "metric.sum:show.runtime"
                  },
                  "children": [
                    {
                      "key": { "kind": "dimension_value", "ref_id": "dim:show.country", "value": "United States", "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "234949", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "806.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "1135.02", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["403", "806", "1209", "1612", "2015", "2418", "2821", "3224", "3627", "4836"]
                        }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": { "kind": "dimension_value", "ref_id": "dim:show.country", "value": "Japan", "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "51584", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "403.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "726.54", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["403", "806", "1209", "1612", "2015", "2418", "3627"]
                        }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": { "kind": "dimension_value", "ref_id": "dim:show.country", "value": "Pakistan", "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "42315", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "403.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "445.42", "values": null },
                        { "metric_id": "metric.values:show.runtime", "value": null, "values": ["403", "806", "2015"] }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": { "kind": "total_other", "ref_id": "dim:show.country", "value": null, "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "203112", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "403.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "568.94", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["403", "806", "1209", "1612", "2015", "2418", "2821", "3627", "5239"]
                        }
                      ],
                      "breakdown": null
                    }
                  ]
                }
              },
              {
                "key": { "kind": "total_other", "ref_id": "dim:show.rating", "value": null, "label": null },
                "metrics": [
                  { "metric_id": "metric.sum:show.runtime", "value": "573469", "values": null },
                  { "metric_id": "metric.median:show.runtime", "value": "403.0", "values": null },
                  { "metric_id": "metric.mean:show.runtime", "value": "724.99", "values": null }
                ],
                "breakdown": null
              }
            ]
          }
        },
        {
          "key": { "kind": "dimension_value", "ref_id": "dim:show.type", "value": "Movie", "label": null },
          "metrics": [
            { "metric_id": "metric.sum:show.runtime", "value": "610057", "values": null },
            { "metric_id": "metric.median:show.runtime", "value": "98.0", "values": null },
            { "metric_id": "metric.mean:show.runtime", "value": "99.58", "values": null }
          ],
          "breakdown": {
            "by": { "how": "dimension_values", "dim_id": "dim:show.rating" },
            "limit": {
              "type": "metric_top_n",
              "from_total_n": 14,
              "kept_n": 2,
              "with_values": "highest",
              "metric_id": "metric.sum:show.runtime"
            },
            "children": [
              {
                "key": { "kind": "dimension_value", "ref_id": "dim:show.rating", "value": "TV-MA", "label": null },
                "metrics": [
                  { "metric_id": "metric.sum:show.runtime", "value": "197725", "values": null },
                  { "metric_id": "metric.median:show.runtime", "value": "96.0", "values": null },
                  { "metric_id": "metric.mean:show.runtime", "value": "95.89", "values": null }
                ],
                "breakdown": {
                  "by": { "how": "dimension_values", "dim_id": "dim:show.country" },
                  "limit": {
                    "type": "metric_top_n",
                    "from_total_n": 68,
                    "kept_n": 3,
                    "with_values": "highest",
                    "metric_id": "metric.sum:show.runtime"
                  },
                  "children": [
                    {
                      "key": { "kind": "dimension_value", "ref_id": "dim:show.country", "value": "United States", "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "55421", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "87.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "84.35", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["22", "24", "26", "28", "29", "30", "32", "33", "34", "36"]
                        }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": { "kind": "dimension_value", "ref_id": "dim:show.country", "value": "India", "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "27555", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "118.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "118.77", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["50", "61", "63", "68", "72", "75", "76", "77", "79", "81"]
                        }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": {
                        "kind": "dimension_value",
                        "ref_id": "dim:show.country",
                        "value": "United Kingdom",
                        "label": null
                      },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "10783", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "92.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "88.39", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["24", "28", "47", "51", "52", "53", "55", "56", "58", "60"]
                        }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": { "kind": "total_other", "ref_id": "dim:show.country", "value": null, "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "103966", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "99.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "98.92", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["12", "29", "30", "31", "33", "34", "35", "36", "38", "44"]
                        }
                      ],
                      "breakdown": null
                    }
                  ]
                }
              },
              {
                "key": { "kind": "dimension_value", "ref_id": "dim:show.rating", "value": "TV-14", "label": null },
                "metrics": [
                  { "metric_id": "metric.sum:show.runtime", "value": "157385", "values": null },
                  { "metric_id": "metric.median:show.runtime", "value": "107.0", "values": null },
                  { "metric_id": "metric.mean:show.runtime", "value": "110.29", "values": null }
                ],
                "breakdown": {
                  "by": { "how": "dimension_values", "dim_id": "dim:show.country" },
                  "limit": {
                    "type": "metric_top_n",
                    "from_total_n": 57,
                    "kept_n": 3,
                    "with_values": "highest",
                    "metric_id": "metric.sum:show.runtime"
                  },
                  "children": [
                    {
                      "key": { "kind": "dimension_value", "ref_id": "dim:show.country", "value": "India", "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "72066", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "131.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "131.03", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["29", "41", "58", "72", "73", "74", "79", "83", "85", "87"]
                        }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": { "kind": "dimension_value", "ref_id": "dim:show.country", "value": "United States", "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "19872", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "87.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "83.5", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["14", "17", "18", "24", "29", "32", "33", "35", "39", "40"]
                        }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": { "kind": "dimension_value", "ref_id": "dim:show.country", "value": "Not Given", "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "8746", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "105.5", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "112.13", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["29", "49", "50", "52", "56", "64", "66", "67", "69", "71"]
                        }
                      ],
                      "breakdown": null
                    },
                    {
                      "key": { "kind": "total_other", "ref_id": "dim:show.country", "value": null, "label": null },
                      "metrics": [
                        { "metric_id": "metric.sum:show.runtime", "value": "56701", "values": null },
                        { "metric_id": "metric.median:show.runtime", "value": "101.0", "values": null },
                        { "metric_id": "metric.mean:show.runtime", "value": "101.07", "values": null },
                        {
                          "metric_id": "metric.values:show.runtime",
                          "value": null,
                          "values": ["5", "12", "32", "36", "37", "45", "46", "47", "48", "50"]
                        }
                      ],
                      "breakdown": null
                    }
                  ]
                }
              },
              {
                "key": { "kind": "total_other", "ref_id": "dim:show.rating", "value": null, "label": null },
                "metrics": [
                  { "metric_id": "metric.sum:show.runtime", "value": "254947", "values": null },
                  { "metric_id": "metric.median:show.runtime", "value": "97.0", "values": null },
                  { "metric_id": "metric.mean:show.runtime", "value": "96.68", "values": null }
                ],
                "breakdown": null
              }
            ]
          }
        }
      ]
    }
  }
}
```

## Catalog

This is accompanied by a catalog (produced outside polars) with lookups that tell
the UI how to display stuff.

For example, the "Total Runtime" metric should tell the UI to **humanize** its durations
up to a maximum unit, *days* (`d`), since the values will be huge.

You might do the same thing for currency values in financial reports, configuring
`humanize: { min_unit: "thousands", max_unit: "billions" }`.

Also, **spark chart** metrics (little inline chart) will need to specify the mark type,
e.g. line, bar, etc.

```json
{
  "catalog": {
    "facts": [
      { "id": "fact:show", "name": "All Show Titles" }
    ],
    "dimensions": [
      { "id": "dim:show.type", "name": "Show Type" },
      { "id": "dim:show.rating", "name": "Rating" },
      { "id": "dim:show.country", "name": "Country" }
    ],
    "measures": [
      { "id": "measure:show.runtime", "name": "Runtime" }
    ],
    "metrics": [
      {
        "id": "metric.sum:show.runtime",
        "name": "Total Runtime",
        "compute": { "op": "aggregate", "expr_id": "measure:show.runtime", "fn": "sum" },
        "result": {
          "kind": "duration",
          "value_type": "i64",
          "unit": "m",
          "format": {
            "type": "duration",
            "humanize": { "min_unit": "m", "max_unit": "d" },
            "decimals": { "type": "auto" }
          }
        }
      },
      {
        "id": "metric.mean:show.runtime",
        "name": "Average Runtime",
        "compute": { "op": "aggregate", "expr_id": "measure:show.runtime", "fn": "mean" },
        "result": {
          "kind": "duration",
          "value_type": "f64",
          "unit": "m",
          "format": { "type": "duration", "humanize": null, "decimals": { "type": "fixed", "n": 1 } }
        }
      },
      {
        "id": "metric.median:show.runtime",
        "name": "Median Runtime",
        "compute": { "op": "aggregate", "expr_id": "measure:show.runtime", "fn": "median" },
        "result": {
          "kind": "duration",
          "value_type": "f64",
          "unit": "m",
          "format": { "type": "duration", "humanize": null, "decimals": { "type": "fixed", "n": 1 } }
        }
      },
      {
        "id": "metric.values:show.runtime",
        "name": "Unique Runtime Values",
        "compute": {
            "op": "aggregate",
            "expr_id": "measure:show.runtime",
            "fn": "unique_values",
            "filter": { "top": 10 }
        },
        "result": {
          "kind": "values",
          "value_type": "f64",
          "format": { "type": "sparkchart", "chart_type": "sparkbar" }
        }
      }
    ]
  }
}
```

## Tree object structure (in TS)

The data model for the report tree is remarkably simple, and can be extended
to nearly any use-case.

Tree we created in Polars:

```ts
type NestedTableReport = {
    catalog: any  // Not defined here, for brevity
    root: ReportRow
}
type ReportRow = {
    key: Report
    metrics: MetricValue[]
    breakdown: ReportBreakdown | null
}
// Every row has a key, with `kind` and `ref_id`.
type RowKey = {
    // kind='total' happens at the root
    // kind='total_other' happens when children are limited
    kind: 'root_total' | 'dimension_value' | 'total_other'
    ref_id: string  // The thing that defines this key
    value: string | null
    label: string | null
}
type MetricValue = {
    metric_id: string
    // We have a strict, "value-is-always-string" contract. Force the client
    // to look up the metric in catalog for how to parse and format it.
    value: string | null
    // An inline chart like a sparkline, for instance, requires multiple values
    values: string[] | null
}
type ReportBreakdown = {
    by: { how: 'dimension_values'; dim_id: string }
    limit: LimitMetricTopN | null
    children: ReportRow[]  // <---- RECURSE
}
type LimitMetricTopN = {
    type: 'metric_top_n'
    from_total_n: number
    kept_n: number
    with_values: 'highest' | 'lowest'
    metric_id: string
}
```


---


When I first wrote this query, it took **3 milliseconds** to produce a
**20,000-line** (indented) JSON tree.

That was too big to show here. So I added the `limit: { type: "metric_top_n" }` feature,
to limit `children` to the ranked top-N, and include a
`key: { kind: "total_other" }` row at the bottom of `children`, with metrics aggregated
separately for the not-included rows. We applied the `limit` logic for two levels in our
tree.

Now the query takes **4 milliseconds**.


## Report Requirements (query we receive from API request)

Make a tree with the hierarchy, **Root Totals -> Type -> Rating -> Country**

- Level 0: **Root totals**
  - Show me the **sum**, **mean**, and **median** of `runtime`
  - Break it down by **Type** (tv or movie).
    - Show me all categories, sorted.
- Level 1: **Type**
  - Show me the **sum**, **mean**, and **median** of `runtime`
  - Break it down by **Rating** (e.g. G, PG, etc.)
    - Show me the **top 2** results with the highest **sum** of `runtime`
    - Also show me the total **sum**, **mean**, and **median** of `runtime`
      of all the other groups not included in the top-N.
- Level 2: **Rating**
  - Show me the **sum**, **mean**, and **median** of `runtime`
  - Break it down by **Country**
    - Show me the **top 3** results with the highest **sum** of `runtime`
    - Also show me the total **sum**, **mean**, and **median** of `runtime`
      of the other groups not included in the top-N.
- Level 3: **Country**
  - Show me the **sum**, **mean**, and **median** of `runtime`
  - Show me a **sparkbar** (little inline bar chart) of the top 10 unique `runtime` values.
  - No breakdown


## Why sum, mean, median, and spark chart?

These represent 4 fundamentally different kinds of metrics, in terms
of how your tree algorithm will handle them.

If you can do all 4, then you can do most things.

- **sum**: Additive. Just aggregate previous sums.
- **mean**: Non-additive, but can be *computed* from additive metrics (sum / count)
  - A **percent-change-since-last-period** is a common example of this kind of metric.
    The 'current-period' and 'prior-period' measures are (almost always) additive.
    **Percent-change** can be computed from them at each level, like how we do **mean**.
- **median**: Non-additive, and *cannot* be computed from additive metrics. We need the
  full-grain fact values every time.
  - A **distinct count** is another example of this kind of metric.
  - That's why you almost never see **median** in reports, and why most engines
    will prefer to *estimate* distinct count instead of computing it exactly.
- **spark chart**: Non-additive, cannot be computed from additive metrics,
  *and* we actually need to put full-grain values in the report output. Or at least
  values from the prior level's grain.


## Full query code

```python
# Assume we start with a plan for the necessary input columns
# with dimensions and measures.
base: pl.LazyFrame = ...
```

I'll hardcode the whole thing as a single `lf =` statement with (almost) no
abstractions, so it's easier to read. But I've structured/named things systematically,
so it's clear how this query can be constructed dynamically.

> [!IMPORTANT]
> **A note on column naming**: When we automate this, input column names will be
> arbitrary. You'll want to **prefix** everything carefully, and use Polars
> `cs` (selectors) API to access them in expressions.
>
> That's why you'll see `metric_sum_runtime` in my code, instead of `runtime_sum`.
> This way, when things go dynamic, we can safely use `cs.starts_with("metric_sum")`
> to access *all* the 'sum' metric columns in the data.
> 
> So, instead of `c.metric_sum_runtime.sum()`, you'd
> use `cs.starts_with("metric_sum").sum()`,


This will *look* like a lot of code, but notice each level of the plan has
nearly the same structure. Your dynamic query-building function will likely
be smaller than this.

```python
# Expressions we will use repeatedly. The redundancy got too annoying.
LIMIT_L1 = 2  # Top N at level 1
LIMIT_L2 = 3  # Top N at level 2
metric_id_rt_sum = lit("metric.sum:show.runtime")
metric_id_rt_median = lit("metric.median:show.runtime")
metric_id_rt_mean = lit("metric.mean:show.runtime")
metric_id_rt_values = lit("metric.values:show.runtime")
dim_id_type = lit("dim:show.type")
dim_id_rating = lit("dim:show.rating")
dim_id_country = lit("dim:show.country")
# Report metric expressions. These we'll also use repeatedly.
report_metric_rt_sum = struct(metric_id=metric_id_rt_sum, value=c.metric_sum_runtime.cast(str), values=lit(None).cast(pl.List(str)))
report_metric_rt_median = struct(metric_id=metric_id_rt_median, value=c.metric_median_runtime.cast(str), values=lit(None).cast(pl.List(str)))
report_metric_rt_mean = struct(metric_id=metric_id_rt_mean, value=c.metric_mean_runtime.round(2).cast(str), values=lit(None).cast(pl.List(str)))
report_metric_rt_values = struct(
    metric_id=metric_id_rt_values,
    value=lit(None).cast(str),
    values=c.measure_runtime.list.unique().list.slice(0, 10).cast(pl.List(str)),
)
# "Other" versions of metrics are needed at level 1 and 2 when we do top-N limits.
report_other_metric_rt_sum = struct(metric_id=metric_id_rt_sum, value=c.other_metric_sum_runtime.cast(str), values=lit(None).cast(pl.List(str)))
report_other_metric_rt_median = struct(metric_id=metric_id_rt_median, value=c.other_metric_median_runtime.cast(str), values=lit(None).cast(pl.List(str)))
report_other_metric_rt_mean = struct(metric_id=metric_id_rt_mean, value=c.other_metric_mean_runtime.round(2).cast(str), values=lit(None).cast(pl.List(str)))
report_other_metric_rt_values = struct(
    metric_id=metric_id_rt_values,
    value=lit(None).cast(str),
    values=c.other_measure_runtime.list.unique().list.slice(0, 10).cast(pl.List(str)),
)
# Metrics per level
report_metrics_l3 = array(
    report_metric_rt_sum,
    report_metric_rt_median,
    report_metric_rt_mean,
    report_metric_rt_values,  # <-- Do sparklines at leaf level
)
report_metrics_l2 = array(
    report_metric_rt_sum,
    report_metric_rt_median,
    report_metric_rt_mean,
)
report_metrics_l1 = array(
    report_metric_rt_sum,
    report_metric_rt_median,
    report_metric_rt_mean,
)
report_metrics_l0 = array(
    report_metric_rt_sum,
    report_metric_rt_median,
    report_metric_rt_mean,
)

# Utility aggregations to unpack into each `agg()` step of our tree.
agg_utils = (
    c.util_metric_count_fact.sum(),
    # Note: 'len()' is used here for the group count. Not sum().
    c.util_metric_count_fact.len().alias('util_metric_count_group'),
)

def limit_slice_other_agg_utils(*, slice: int) -> tuple[pl.Expr, ...]:
    """
    'other' metrics are needed when we do a top-N, or some other kind of limit, on aggregated
    report children. We need to compute our utility metrics for other items beyond that limit.
    These need separate names that don't conflict with the main utility metrics.
    """
    return (
        c.util_metric_count_fact.slice(slice).sum().alias('other_util_metric_count_fact'),
        # Note: 'len()' is used here for the group count. Not sum().
        c.util_metric_count_fact.slice(slice).len().alias('other_util_metric_count_group'),
    )


def append_to_list(lst: pl.Expr, *items: pl.Expr) -> pl.Expr:
    # Just makes the code clearer at the call site
    return lst.list.concat(pl.concat_list(*items))


# Build tree
# ###################################################################################
# ###################################################################################
lf = (
    base
    # Prep: Initialize additive metrics that will move up all tree levels
    .with_columns(
        # Utility
        util_metric_count_fact=lit(1),
        # Dynamic
        metric_sum_runtime=c.measure_runtime,
    )
    # -----------------------------------------------------------------------------------
    # Level 3: Root -> Type -> Rating -> Country
    # ===================================================================================
    # 1. Aggregate (no sort)
    .group_by(
        c.dim_type,
        c.dim_rating,
        c.dim_country,
    )
    .agg(
        *agg_utils,
        c.measure_runtime,  # At leaf level, accumulate facts into lists, for median calc
        c.metric_sum_runtime.sum(),
        children=None,
        metric_median_runtime=c.measure_runtime.median(),  # At leaf level, compute here instead of step 2
    )
    # 2. Computed metrics
    .with_columns(
        metric_mean_runtime=c.metric_sum_runtime / c.util_metric_count_fact,
    )
    # 3. Pack report
    .with_columns(
        # Gets accumulated into `children` of next level up
        report=struct(
            key=struct(kind=lit('dimension_value'), ref_id=dim_id_country, value=c.dim_country, label=None),
            metrics=report_metrics_l3,
            breakdown=None,
        )
    )
    # -----------------------------------------------------------------------------------
    # Level 2: Root -> Type -> Rating
    # ===================================================================================
    # 1. Aggregate
    .sort(c.metric_sum_runtime, c.dim_country, descending=True)
    .group_by(
        c.dim_type,
        c.dim_rating,
    )
    .agg(
        *agg_utils,
        c.measure_runtime.flatten(),  # For median calc, flatten child lists
        c.metric_sum_runtime.sum(),
        # Children
        *limit_slice_other_agg_utils(slice=LIMIT_L2),
        children=c.report.limit(LIMIT_L2),
        # Other children:
        other_measure_runtime=c.measure_runtime.slice(LIMIT_L2).flatten(),
        other_metric_sum_runtime=c.metric_sum_runtime.slice(LIMIT_L2).sum(),
    )
    # 2. Computed metrics
    .with_columns(
        # Overall metrics for current report
        metric_mean_runtime=c.metric_sum_runtime / c.util_metric_count_fact,
        metric_median_runtime=c.measure_runtime.list.median(),
        # "Other" metrics to append to the children reports list
        other_metric_mean_runtime=c.other_metric_sum_runtime / c.other_util_metric_count_fact,
        other_metric_median_runtime=c.other_measure_runtime.list.median(),
    )
    # 3. Pack report
    .with_columns(
        report=struct(
            key=struct(kind=lit('dimension_value'), ref_id=dim_id_rating, value=c.dim_rating, label=None),
            metrics=report_metrics_l2,
            breakdown=when(c.other_util_metric_count_group == 0)
            .then(
                struct(
                    by=struct(how=lit("dimension_values"), dim_id=dim_id_country),
                    limit=None,
                    children=c.children,
                )
            )
            .otherwise(
                struct(
                    by=struct(how=lit("dimension_values"), dim_id=dim_id_country),
                    limit=struct(
                        type=lit("metric_top_n"),
                        from_total_n=c.util_metric_count_group,
                        kept_n=lit(LIMIT_L2),
                        with_values=lit("highest"),
                        metric_id=metric_id_rt_sum,
                    ),
                    children=append_to_list(
                        c.children,
                        struct(
                            key=struct(kind=lit('total_other'), ref_id=dim_id_country, value=None, label=None),
                            metrics=report_metrics_l3,  # <-- Previous level's metrics. Not current.
                            breakdown=None,
                        )
                    ),
                )
            ),
        ),
    )
    # -----------------------------------------------------------------------------------
    # Level 1: Root -> Type
    # ===================================================================================
    # 1. Aggregate
    .sort(c.metric_sum_runtime, c.dim_rating, descending=True)
    .group_by(
        c.dim_type,
    )
    .agg(
        *agg_utils,
        c.measure_runtime.flatten(),
        c.metric_sum_runtime.sum(),
        # Children
        *limit_slice_other_agg_utils(slice=LIMIT_L1),
        children=c.report.limit(LIMIT_L1),
        # Other children
        other_measure_runtime=c.measure_runtime.slice(LIMIT_L1).flatten(),
        other_metric_sum_runtime=c.metric_sum_runtime.slice(LIMIT_L1).sum(),
    )
    # 2. Computed metrics
    .with_columns(
        # Overall metrics for current report
        metric_mean_runtime=c.metric_sum_runtime / c.util_metric_count_fact,
        metric_median_runtime=c.measure_runtime.list.median(),
        # "Other" metrics to append to the children reports list
        other_metric_mean_runtime=c.other_metric_sum_runtime / c.other_util_metric_count_fact,
        other_metric_median_runtime=c.other_measure_runtime.list.median(),
    )
    # 3. Pack report
    .with_columns(
        report=struct(
            key=struct(kind=lit('dimension_value'), ref_id=dim_id_type, value=c.dim_type, label=None),
            metrics=array(report_metric_rt_sum, report_metric_rt_median, report_metric_rt_mean),
            breakdown=when(c.other_util_metric_count_group == 0)
            .then(
                struct(
                    by=struct(how=lit("dimension_values"), dim_id=dim_id_rating),
                    limit=None,
                    children=c.children,
                )
            )
            .otherwise(
                struct(
                    by=struct(how=lit("dimension_values"), dim_id=dim_id_rating),
                    limit=struct(
                        type=lit("metric_top_n"),
                        from_total_n=c.util_metric_count_group,
                        kept_n=lit(LIMIT_L1),
                        with_values=lit("highest"),
                        metric_id=metric_id_rt_sum,
                    ),
                    children=append_to_list(
                        c.children,
                        struct(
                            key=struct(kind=lit('total_other'), ref_id=dim_id_rating, value=None, label=None),
                            metrics=report_metrics_l2,  # <-- Previous level's metrics. Not current.
                            breakdown=None,
                        )
                    ),
                )
            ),
        ),
    )
    # -----------------------------------------------------------------------------------
    # Level 0: Root
    # ===================================================================================
    # 1. Aggregate
    .sort(c.metric_sum_runtime, c.dim_type, descending=True)
    .group_by(
        root=lit('root'),
    )
    .agg(
        *agg_utils,
        c.measure_runtime.flatten(),
        c.metric_sum_runtime.sum(),
        # Children
        children=c.report,
    )
    # 2. Computed metrics
    .with_columns(
        metric_mean_runtime=c.metric_sum_runtime / c.util_metric_count_fact,
        metric_median_runtime=c.measure_runtime.list.median(),
    )
    # 3. Pack report
    .with_columns(
        report=struct(
            key=struct(kind=lit('root_total'), ref_id=lit('fact:show'), value=None, label=None),
            metrics=report_metrics_l0,
            breakdown=struct(
                by=struct(how=lit("dimension_values"), dim_id=dim_id_type),
                limit=None,
                children=c.children,
            ),
        )
    )
    # -----------------------------------------------------------------------------------
    # Finish: pack into root item, i.e. `shape: (1, 1)`
    # ===================================================================================
    .select(root=c.report)
)
```

And that's it.

There isn't another SQL engine on the planet that can make this process as
ergonomic as it is in Polars. 

---

A couple things that might be unintuitive:

1. *Wtf is a "utility" metric?* These are metrics that don't depend on the input
   data. For example, the 'count fact' util metric is a running count of the rows
   in the original data. We initialize it as `lit(1)`, and `.sum()` it up the tree.
   It's needed for many things, including `mean` metrics.

2. *The 'count group' metric confuses me.* This one uses `.len()` to get a row count
   at the *current* (i.e. most recent) `.agg()` level. This lets us know, when packing a `report=...`,
   how many rows were aggregated per group in the most recent `.agg()`. This was needed
   for our `limit` feature. It goes into `breakdown.limit.from_total_n`, but, more importantly,
   we need the `other_` version of the group count to make a `when().then().otherwise()`
   decision on whether to append a `kind='total_other'` row to that report's children.
   If a given group is smaller than the limit, then there's no need for an 'other' row.

3. Notice what we did with our `measure_` columns (not metrics) in the aggregations:

   ```python
   .agg(
       c.measure_runtime,
   )
   ...
   .agg(
       c.measure_runtime.flatten(),
   )
   ...
   .agg(
       c.measure_runtime.flatten(),
   )
   ...
   .agg(
       c.measure_runtime.flatten(),
   )
   ```

   To 'carry a measure up the tree' means to give us access to the fact-level values
   as a list. This lets us compute non-additive metrics like median or sparkcharts
   **without** having to join subqueries.

   - `measure_runtime` is initially `i64` in the input data.
   - The initial `agg(measure_runtime)` aggregates it into `list[i64]`. 
   - The subsequent `.flatten()` aggregations combines the lists in a group
     into one list, ensuring the column remains `list[i64]` instead of creating
     nested lists.
