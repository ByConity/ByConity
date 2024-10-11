drop table if exists events;
CREATE TABLE events
(
    `app_id` UInt32,
    `app_name` String,
    `device_id` String,
    `content` String,
    `ab_version` Array(Int32) BLOOM,
    `string_params` Map(String, String)
)
ENGINE = CnchMergeTree
PARTITION BY (app_id, app_name)
ORDER BY (app_id, app_name, device_id);

insert into events values ('1', '2', '3', '{}', [], {});

drop table if exists mv_events_target;
CREATE TABLE mv_events_target
(
    `app_id` UInt32,
    `app_name` String,
    `device_id` String,
    `content` String,
    `ab_version` Array(Int32) BLOOM,
    `string_params` Map(String, String),
    `agent_id` String
)
ENGINE = CnchMergeTree
PARTITION BY (app_id, app_name)
ORDER BY (app_id, app_name, device_id);

CREATE MATERIALIZED VIEW mv_events TO mv_events_target
(
    `app_id` UInt32,
    `app_name` String,
    `device_id` String,
    `content` String,
    `ab_version` Array(Int32) BLOOM,
    `string_params` Map(String, String),
    `agent_id` String
) AS
SELECT
    app_id,
    app_name,
    device_id,
    content,
    ab_version,
    string_params,
    JSONExtractString(content, '$.string_params.agent_id') AS agent_id
FROM events;

insert into events values ('2', '3', '4', '{}', [], {});
insert into events values ('3', '4', '5', '{}', [], {});
insert into events values ('4', '4', '5', '{}', [], {});

select '1. test different aggregate';
SELECT
    device_id,
    app_name,
    count(1),
    min(app_id),
    countDistinct(JSONExtractString(content, '$.string_params.agent_id')),
    countDistinct(JSONExtractString(content, '$.string_params.agent_id'), content),
    finalizeAggregation(countDistinctState(JSONExtractString(content, '$.string_params.agent_id'), content)),
    finalizeAggregation(countState())
FROM events
GROUP BY device_id, app_name
ORDER BY device_id, app_name
SETTINGS enable_materialized_view_rewrite=0;

SELECT
    device_id,
    app_name,
    count(1),
    min(app_id),
    countDistinct(JSONExtractString(content, '$.string_params.agent_id')),
    countDistinct(JSONExtractString(content, '$.string_params.agent_id'), content),
    finalizeAggregation(countDistinctState(JSONExtractString(content, '$.string_params.agent_id'), content)),
    finalizeAggregation(countState())
FROM events
GROUP BY device_id, app_name
ORDER BY device_id, app_name
SETTINGS enforce_materialized_view_rewrite=1;

select '2. partition filter';
SELECT
    device_id,
    count(1)
FROM events
WHERE ((app_id IN (2)) AND (app_name = '3'))
GROUP BY device_id
ORDER BY device_id
SETTINGS enable_materialized_view_rewrite=0;

SELECT
    device_id,
    count(1)
FROM events
WHERE ((app_id IN (2)) AND (app_name = '3'))
GROUP BY device_id
ORDER BY device_id
SETTINGS enforce_materialized_view_rewrite=1;

select '3. partition filter with no data';
SELECT
    device_id,
    count(1)
FROM events
WHERE ((app_id IN (1)) AND (app_name = '2'))
GROUP BY device_id
ORDER BY device_id
SETTINGS enable_materialized_view_rewrite=0;

SELECT
    device_id,
    count(1)
FROM events
WHERE ((app_id IN (1)) AND (app_name = '2'))
GROUP BY device_id
ORDER BY device_id
SETTINGS enforce_materialized_view_rewrite=1;

select '3. no aggregate';
SELECT
    app_id,
    app_name,
    JSONExtractString(content, '$.string_params.agent_id')
FROM events
ORDER BY app_id, app_name
SETTINGS enable_materialized_view_rewrite=0;

SELECT
    app_id,
    app_name,
    JSONExtractString(content, '$.string_params.agent_id')
FROM events
ORDER BY app_id, app_name
SETTINGS enforce_materialized_view_rewrite=1;

select '4. no aggregate with filter';
SELECT
    device_id,
    JSONExtractString(content, '$.string_params.agent_id')
FROM events
WHERE ((app_id IN (2)) AND (app_name = '3'))
ORDER BY device_id
SETTINGS enable_materialized_view_rewrite=0;

SELECT
    device_id,
    JSONExtractString(content, '$.string_params.agent_id')
FROM events
WHERE ((app_id IN (2)) AND (app_name = '3'))
ORDER BY device_id
SETTINGS enforce_materialized_view_rewrite=1;