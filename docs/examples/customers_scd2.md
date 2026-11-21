# Customer history with scd2

A CRM exports the full customer list every day. Customers move, change tier and leave. You want
a dimension that answers "what did we know about this customer on a given day", not only "what
is true now". This example builds that dimension with the `scd2` write strategy, from three daily
snapshots, on DuckDB, polars or pandas.

## The data

`examples/customers_scd2/data/` holds three small CSV files with the same five columns
(`id, name, city, tier, status`).

| Snapshot | What changed since the day before |
|---|---|
| day1 | Five customers, all active. |
| day2 | Customer 1 moves from Recife to Olinda. Customer 2 goes from basic to gold. |
| day3 | Customer 4 is closed. Customer 6 is new. |

## The model

The whole pipeline is one model. It returns the snapshot unchanged, and the write strategy does
the versioning.

```python
--8 < --"examples/customers_scd2/models/dim_customers.py:model"
```

`unique_key` is the customer, `tracked_columns` are the columns whose change opens a new
version, and `deletes` says that a row with status `closed` closes the customer's current version
without opening another. See [Merge, replace_partitions, scd2 and schema
changes](../guide/write-strategies.md) for every rule.

The project file picks the snapshot with a variable and declares one target per engine:

```yaml
--8<-- "examples/customers_scd2/pipeloom.yml"
```

## Run it

Day 1. The run id and start time are fixed here so that your output matches this page. A real
pipeline leaves both out, and every version then opens at the run's real start time.

```console
$ uv run pipeloom run --project-dir examples/customers_scd2 --vars '{"day": "day1"}' \
    --run-id day1 --run-started-at 2026-01-01T00:00:00Z
$ uv run examples/customers_scd2/show.py
```

```text
--8<-- "examples/customers_scd2/expected/dim_customers_day1.csv"
```

Every customer has one open version: `valid_to` is empty and `is_current` is true.

Day 2:

```console
$ uv run pipeloom run --project-dir examples/customers_scd2 --vars '{"day": "day2"}' \
    --run-id day2 --run-started-at 2026-01-02T00:00:00Z
$ uv run examples/customers_scd2/show.py
```

```text
--8<-- "examples/customers_scd2/expected/dim_customers_day2.csv"
```

Customers 1 and 2 each have a closed version and a new open one. The run reports
`rows_inserted: 2` (versions opened) and `rows_updated: 2` (versions closed by a change). The
other three customers are unchanged and get no new row.

Day 3:

```console
$ uv run pipeloom run --project-dir examples/customers_scd2 --vars '{"day": "day3"}' \
    --run-id day3 --run-started-at 2026-01-03T00:00:00Z
$ uv run examples/customers_scd2/show.py
```

```text
--8<-- "examples/customers_scd2/expected/dim_customers_day3.csv"
```

Customer 4 has a closed version and no open one (`rows_deleted: 1`), and customer 6 has its first
version. Running day 3 again with the same start time changes nothing, because versions open and
close at the run's start time.

To use another engine, add `--target polars` or `--target pandas` to the run and to `show.py`.
All three give the same rows.

## Read it as of a date

A version was valid at a moment when `valid_from <= moment` and `valid_to` is empty or after the
moment. On DuckDB, after day 3:

```sql
select id, name, city, tier
from crm.crm.dim_customers
where valid_from <= timestamptz '2026-01-01 12:00:00+00'
  and (valid_to is null or valid_to > timestamptz '2026-01-01 12:00:00+00')
order by id;
```

Among the rows, noon on 2026-01-01 gives customer 1 in Recife and customer 2 on basic; noon on
2026-01-03 gives customer 1 in Olinda, customer 2 on gold and no customer 4. The current dimension is the same
query with `where is_current`.

## How it is tested

`tests/examples/test_walkthrough_customers_scd2.py` runs the three days on all three engines
with fixed start times and compares the table with the committed rows in
`examples/customers_scd2/expected/`. It also checks the row counts of each run, the idempotent
rerun and the as-of query.

## What to try next

- Add `tier` to a snapshot only for customer 3 and see that `name` stays out of the new version.
- Remove `city` from `tracked_columns`: a move no longer opens a version.
- Run day 3 with `--full-refresh`: the history is dropped and each row becomes the first version.
- Add a `status: closed` row for customer 4 again in a day 4 file: a delete for a customer with
  no open version is ignored.
