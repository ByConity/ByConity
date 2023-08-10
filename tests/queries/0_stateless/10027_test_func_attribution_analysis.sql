use test;
drop table if exists test_array_associated;
CREATE TABLE test_array_associated
(
    `time` UInt64,
    `name` String,
    `string_params` Map(String, String),
    `int_params` Map(String, Int64),
    `float_params` Map(String, Float64),
    `string_item_profiles` Map(String, Array(String)),
    `int_item_profiles` Map(String, Array(Int64)),
    `float_item_profiles` Map(String, Array(Float64))
)
ENGINE = CnchMergeTree()
PARTITION BY time
ORDER BY time;

insert into test_array_associated(time, name, string_params, int_params, float_params, string_item_profiles, int_item_profiles, float_item_profiles) values(1000, 'start', {'M':'ll', 'N':'aa'}, {'P':5, 'Q':10}, {}, {'S': ['aa', 'bb']}, {'I':[1, 2]}, {'F':[1.1, 1.2]});
insert into test_array_associated(time, name, string_params, int_params, float_params, string_item_profiles, int_item_profiles, float_item_profiles) values(1111, 'launch', {'M':'bb', 'N':'aa'}, {'P':10, 'Q':5}, {}, {'S': ['aa', 'bb']}, {'I':[1, 2]}, {'F':[1.1, 1.2]});
insert into test_array_associated(time, name, string_params, int_params, float_params, string_item_profiles, int_item_profiles, float_item_profiles) values(1555, 'pro', {'M':'bb', 'N':'aa'}, {'P':5, 'Q':5}, {'A' : 1.5}, {'S': ['aa', 'bb']}, {'I':[1, 2]}, {'F':[1.1, 1.2]});
insert into test_array_associated(time, name, string_params, int_params, float_params, string_item_profiles, int_item_profiles, float_item_profiles) values(1565, 'start', {'M':'ll', 'N':'bb'}, {'P':10, 'Q':10}, {}, {'S': ['aa', 'cc']}, {'I':[1, 2]}, {'F':[1.1, 1.2]});
insert into test_array_associated(time, name, string_params, int_params, float_params, string_item_profiles, int_item_profiles, float_item_profiles) values(1666, 'launch', {'M':'cc', 'N':'cc'}, {'P':5, 'Q':10}, {}, {'S': ['aa', 'bb']}, {'I':[1, 2]}, {'F':[1.1, 1.2]});
insert into test_array_associated(time, name, string_params, int_params, float_params, string_item_profiles, int_item_profiles, float_item_profiles) values(2222, 'pro', {'M':'ll', 'N':'cc'}, {'P':5, 'Q':1}, {'A' : 1.6}, {'S': ['cc', 'bb']}, {'I':[1, 2]}, {'F':[1.1, 1.2]});
insert into test_array_associated(time, name, string_params, int_params, float_params, string_item_profiles, int_item_profiles, float_item_profiles) values(3000, 'orange', {'M':'ll', 'N':'aa'}, {'P':5, 'Q':10}, {'A' : 1.3}, {'S': ['aa', 'bb']}, {'I':[1, 2]}, {'F':[1.1, 1.2]});
insert into test_array_associated(time, name, string_params, int_params, float_params, string_item_profiles, int_item_profiles, float_item_profiles) values(3111, 'start', {'M':'ll', 'N':'aa'}, {'P':5, 'Q':10}, {}, {'S': ['aa', 'bb']}, {'I':[1, 2]}, {'F':[1.1, 1.2]});
insert into test_array_associated(time, name, string_params, int_params, float_params, string_item_profiles, int_item_profiles, float_item_profiles) values(3333, 'launch', {'M':'ll', 'N':'aa'}, {'P':5, 'Q':10}, {}, {'S': ['aa', 'dd']}, {'I':[1, 2]}, {'F':[1.1, 1.2]});
insert into test_array_associated(time, name, string_params, int_params, float_params, string_item_profiles, int_item_profiles, float_item_profiles) values(4444, 'pro', {'M':'ll', 'N':'aa'}, {'P':1, 'Q':1}, {'A': 1.1}, {'S': ['aa', 'bb']}, {'I':[1, 2, 5]}, {'F':[1.1, 1.3]});
insert into test_array_associated(time, name, string_params, int_params, float_params, string_item_profiles, int_item_profiles, float_item_profiles) values(5555, 'tar', {'M':'ll', 'N':'aa'}, {'P':5, 'Q':10}, {'A': 1.3}, {'S': ['aa', 'bb']}, {'I':[2, 1]}, {'F':[1.2, 1.3, 1.5]});

SELECT attributionAnalysis('tar', ['launch', 'pro'], [''], 5000, 3, 1, [0], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), assumeNotNull(params)) AS inner
FROM
(
    SELECT
        time,
        name,
        int_params['P'] AS p,
        int_item_profiles['I'] AS arr,
        string_item_profiles['S'] AS str,
        string_params['M'] AS params,
        float_params['A'] AS flo,
        float_item_profiles['F'] AS fin
    FROM test_array_associated
    ORDER BY time ASC
);
SELECT attributionAnalysis('tar', ['launch', 'pro'], [''], 5000, 3, 1, [0], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), multiIf(name='launch', assumeNotNull(params), multiIf(name='pro', assumeNotNull(par), ''))) AS inner
FROM
(
    SELECT
        time,
        name,
        int_params['P'] AS p,
        int_item_profiles['I'] AS arr,
        string_item_profiles['S'] AS str,
        string_params['M'] AS params,
        string_params['N'] AS par,
        float_params['A'] AS flo,
        float_item_profiles['F'] AS fin
    FROM test_array_associated
    ORDER BY time ASC
);
SELECT attributionAnalysis('tar', ['launch', 'pro'], [''], 5000, 3, 1, [0], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), '') AS inner
FROM
(
    SELECT
        time,
        name,
        int_params['P'] AS p,
        int_item_profiles['I'] AS arr,
        string_item_profiles['S'] AS str,
        string_params['M'] AS params,
        string_params['N'] AS par,
        float_params['A'] AS flo,
        float_item_profiles['F'] AS fin
    FROM test_array_associated
    ORDER BY time ASC
);
SELECT attributionAnalysis('tar', ['launch'], [''], 5000, 3, 1, [0], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), assumeNotNull(params)) AS inner
FROM
(
    SELECT
        time,
        name,
        int_params['P'] AS p,
        int_item_profiles['I'] AS arr,
        string_item_profiles['S'] AS str,
        string_params['M'] AS params,
        float_params['A'] AS flo,
        float_item_profiles['F'] AS fin
    FROM test_array_associated
    ORDER BY time ASC
);
SELECT attributionAnalysis('tar', ['launch'], ['pro'], 5000, 3, 1, [0], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), assumeNotNull(params)) AS inner
FROM
(
    SELECT
        time,
        name,
        int_params['P'] AS p,
        int_item_profiles['I'] AS arr,
        string_item_profiles['S'] AS str,
        string_params['M'] AS params,
        float_params['A'] AS flo,
        float_item_profiles['F'] AS fin
    FROM test_array_associated
    ORDER BY time ASC
);
SELECT attributionAnalysis('tar', ['launch'], ['pro'], 5000, 3, 1, [1, 0, 0], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), assumeNotNull(params), assumeNotNull(p)) AS inner
FROM
(
    SELECT
        time,
        name,
        int_params['P'] AS p,
        int_item_profiles['I'] AS arr,
        string_item_profiles['S'] AS str,
        string_params['M'] AS params,
        float_params['A'] AS flo,
        float_item_profiles['F'] AS fin
    FROM test_array_associated
    ORDER BY time ASC
);
SELECT attributionAnalysis('tar', ['launch'], ['pro'], 5000, 3, 1, [2, 1, 0], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), assumeNotNull(params), assumeNotNull(p), assumeNotNull(q)) AS inner
FROM
(
    SELECT
        time,
        name,
        int_params['P'] AS p,
        int_params['Q'] AS q,
        int_item_profiles['I'] AS arr,
        string_item_profiles['S'] AS str,
        string_params['M'] AS params,
        float_params['A'] AS flo,
        float_item_profiles['F'] AS fin
    FROM test_array_associated
    ORDER BY time ASC
);
SELECT attributionAnalysis('tar', ['launch'], ['pro'], 5000, 3, 1, [2, 0, 1], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), assumeNotNull(params), assumeNotNull(flo), assumeNotNull(fin)) AS inner
FROM
(
    SELECT
        time,
        name,
        int_params['P'] AS p,
        int_item_profiles['I'] AS arr,
        string_item_profiles['S'] AS str,
        string_params['M'] AS params,
        float_params['A'] AS flo,
        float_item_profiles['F'] AS fin
    FROM test_array_associated
    ORDER BY time ASC
);
SELECT attributionAnalysis('tar', ['start'], ['pro', 'launch'], 5000, 3, 1, [0], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), assumeNotNull(params), assumeNotNull(flo), assumeNotNull(fin)) AS inner
FROM
(
    SELECT
        time,
        name,
        int_params['P'] AS p,
        int_item_profiles['I'] AS arr,
        string_item_profiles['S'] AS str,
        string_params['M'] AS params,
        float_params['A'] AS flo,
        float_item_profiles['F'] AS fin
    FROM test_array_associated
    ORDER BY time ASC
);
SELECT attributionAnalysis('tar', ['start'], ['pro', 'launch'], 5000, 3, 1, [2, 0, 0, 0, 1], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p),  assumeNotNull(params), assumeNotNull(p), assumeNotNull(q)) AS inner
FROM
(
    SELECT
        time,
        name,
        int_params['P'] AS p,
        int_params['Q'] AS q,
        int_item_profiles['I'] AS arr,
        string_item_profiles['S'] AS str,
        string_params['M'] AS params,
        float_params['A'] AS flo,
        float_item_profiles['F'] AS fin
    FROM test_array_associated
    ORDER BY time ASC
);
SELECT attributionAnalysis('tar', ['start'], ['pro', 'orange', 'launch'], 5000, 3, 1, [2, 0, 0, 1, 1], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), assumeNotNull(params), assumeNotNull(p), assumeNotNull(q)) AS inner
FROM
(
    SELECT
        time,
        name,
        int_params['P'] AS p,
        int_params['Q'] AS q,
        int_item_profiles['I'] AS arr,
        string_item_profiles['S'] AS str,
        string_params['M'] AS params,
        float_params['A'] AS flo,
        float_item_profiles['F'] AS fin
    FROM test_array_associated
    ORDER BY time ASC
);

SELECT attributionCorrelationFuse(10)(inner)
FROM
(
    SELECT attributionAnalysis('tar', ['launch', 'pro'], [''], 5000, 3, 1, [0], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), assumeNotNull(params)) AS inner
    FROM
    (
        SELECT
            time,
            name,
            int_params['P'] AS p,
            int_item_profiles['I'] AS arr,
            string_item_profiles['S'] AS str,
            string_params['M'] AS params,
            float_params['A'] AS flo,
            float_item_profiles['F'] AS fin
        FROM test_array_associated
        ORDER BY time ASC
    )
);

SELECT attributionAnalysisFuse(10)(inner)
FROM
(
    SELECT attributionAnalysis('tar', ['launch', 'pro'], [''], 5000, 3, 1, [0], 'Asia/Shanghai', 0.05, 0.8, 0.15)(time, name, assumeNotNull(p), assumeNotNull(params)) AS inner
    FROM
    (
        SELECT
            time,
            name,
            int_params['P'] AS p,
            int_item_profiles['I'] AS arr,
            string_item_profiles['S'] AS str,
            string_params['M'] AS params,
            float_params['A'] AS flo,
            float_item_profiles['F'] AS fin
        FROM test_array_associated
        ORDER BY time ASC
    )
);

DROP TABLE test_array_associated;