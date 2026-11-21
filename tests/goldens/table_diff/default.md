### Table diff: orders

Source (reference) `main.orders`, target `dev_main.orders`, key `id`.

|  | Rows |
|---|---|
| Source rows | 8 |
| Target rows | 6 |
| Matched | 2 |
| Partial | 2 |
| Source only | 1 |
| Target only | 1 |
| Duplicate keys (source / target) | 1 / 0 |
| Null keys (source / target) | 1 / 0 |

Schema changes:

| Column | Change | Source | Target |
|---|---|---|---|
| note | added |  | string |

Columns that differ:

| Column | Rows |
|---|---|
| name | 1 |
| amount | 1 |
