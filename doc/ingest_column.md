# Column ingestion

## Introduction
Column ingestion query updates values of a column in a partition of a table by taking the value of another column from another table

Example:
Table `db.target`

| Date          |  ID      | Name  |  Income |
|:--------------|:--------:|:-----:|--------:|
| 2021-01-01    |   1      | A     |  4      |
| 2021-01-01    |   2      | B     |  5      |
| 2021-01-01    |   3      | C     |  6      |

Table `db.source`

| Date          |  ID      | Name  |
|:--------------|:--------:|------:|
| 2021-01-01    |   1      | aa    |
| 2021-01-01    |   2      | bb    |
| 2021-01-01    |   4      | cc    |

```
ALTER TABLE db.target INGEST PARTITION '2021-01-01' COLUMNS Name KEY ID FROM db.source
```

Table `db.target` after execute the query

| Date          |  ID      | Name  |  Income |
|:--------------|:--------:|:-----:|--------:|
| 2021-01-01    |   1      | aa    |  4      |
| 2021-01-01    |   2      | bb    |  5      |
| 2021-01-01    |   3      |       |  6      |
| 2021-01-01    |   4      | dd    |  0      |

The Name value of row with ID = 3 can have value remain to C if the setting for target table ingest_default_column_value_if_not_provided is 0 by creating the table with this syntax

```
CREATE TABLE db.target
(
    `date` Date,
    `id` Int32,
    `name` String
)
ENGINE = CnchMergeTree
PARTITION BY date
ORDER BY id
SETTINGS ingest_default_column_value_if_not_provided = 0
```

In that case, the result will be

| Date          |  ID      | Name  |  Income |
|:--------------|:--------:|:-----:|--------:|
| 2021-01-01    |   1      | aa    |  4      |
| 2021-01-01    |   2      | bb    |  5      |
| 2021-01-01    |   3      | C     |  6      |
| 2021-01-01    |   4      | dd    |  0      |



## Syntax
```
ALTER TABLE db.target INGEST PARTITION xxx COLUMNS col1, col2 [KEY k1, k2] FROM db.temp_table
```

## Implementation
The main execution of ingest column will be done in worker
