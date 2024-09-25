CREATE TABLE test_dump.max_avg_parition_by  (
    id Int,
    department Nullable(String),
    onboard_date Nullable(String),
    age Nullable(Int)
) ENGINE = Distributed('', 'test_dump', 'max_avg_parition_by_local', id);

CREATE TABLE test_dump.max_avg_parition_by_local  (
    id Int,
    department Nullable(String),
    onboard_date Nullable(String),
    age Nullable(Int)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE test_dump.people (
    id Int,
    department Nullable(String),
    onboard_date Nullable(String),
    age Nullable(Int)
) ENGINE = Distributed('', 'test_dump', 'people_local', id);

CREATE TABLE test_dump.people_local (
    id Int,
    department Nullable(String),
    onboard_date Nullable(String),
    age Nullable(Int)
) ENGINE = MergeTree() ORDER BY id;