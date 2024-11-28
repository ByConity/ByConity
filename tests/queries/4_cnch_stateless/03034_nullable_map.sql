DROP TABLE IF EXISTS 03034_nullable_map;

CREATE TABLE 03034_nullable_map (a Int, m1 Nullable(Map(String, Int)) KV, m2 Map(String, Map(String, Int)) KV) ENGINE = CnchMergeTree ORDER BY a;

INSERT INTO 03034_nullable_map VALUES (0, {}, {}) (1, NULL, {'k1': {}}) (2, {'k1': 1}, {'k1': {'kk1': 1}});

SELECT * FROM 03034_nullable_map ORDER BY a;

SELECT 'mapElement';
WITH 'k1' AS k SELECT m1{k}, m2{k} FROM 03034_nullable_map ORDER BY a;
WITH 'k2' AS k SELECT m1{k}, m2{k} FROM 03034_nullable_map ORDER BY a;
WITH 'k1'::Nullable(String) AS k SELECT m1{k}, m2{k} FROM 03034_nullable_map ORDER BY a;
WITH 'k2'::Nullable(String) AS k SELECT m1{k}, m2{k} FROM 03034_nullable_map ORDER BY a;
SELECT m1{NULL}, m2{NULL} FROM 03034_nullable_map ORDER BY a;

SELECT 'arrayElement';
WITH 'k1' AS k SELECT m1[k], m2[k] FROM 03034_nullable_map ORDER BY a;
WITH 'k2' AS k SELECT m1[k], m2[k] FROM 03034_nullable_map ORDER BY a;
WITH 'k1'::Nullable(String) AS k SELECT m1[k], m2[k] FROM 03034_nullable_map ORDER BY a;
WITH 'k2'::Nullable(String) AS k SELECT m1[k], m2[k] FROM 03034_nullable_map ORDER BY a;
SELECT m1[NULL], m2[NULL] FROM 03034_nullable_map ORDER BY a;

SELECT 'cast';
SELECT cast(m1, 'Nullable(Map(String, String))'), cast(m2, 'Map(String, Map(String, String))'), cast(m2, 'Nullable(Map(String, String))') FROM 03034_nullable_map ORDER BY a;

SELECT 'mapKeys';
WITH map('k1', 'k2')::Map(String, String) AS m SELECT mapKeys(m), toTypeName(mapKeys(m));
WITH map('k1', 'k2')::Nullable(Map(String, String)) AS m SELECT mapKeys(m), toTypeName(mapKeys(m));
SELECT mapKeys(m1) AS mk1, toTypeName(mk1), mapKeys(m2) AS mk2, toTypeName(mk2) FROM 03034_nullable_map ORDER BY a;

SELECT 'mapValues';
WITH map('k1', 'k2')::Map(String, String) AS m SELECT mapValues(m), toTypeName(mapValues(m));
WITH map('k1', 'k2')::Nullable(Map(String, String)) AS m SELECT mapValues(m), toTypeName(mapValues(m));
SELECT mapValues(m1) AS mv1, toTypeName(mv1), mapValues(m2) AS mv2, toTypeName(mv2) FROM 03034_nullable_map ORDER BY a;

SELECT 'mapContains';
WITH 'k1' AS k SELECT mapContains(m1, k) AS mc1, toTypeName(mc1), mapContains(m2, k) AS mc2, toTypeName(mc2) FROM 03034_nullable_map ORDER BY a;
WITH 'k2' AS k SELECT mapContains(m1, k) AS mc1, toTypeName(mc1), mapContains(m2, k) AS mc2, toTypeName(mc2) FROM 03034_nullable_map ORDER BY a;
WITH 'k1'::Nullable(String) AS k SELECT mapContains(m1, k) AS mc1, toTypeName(mc1), mapContains(m2, k) AS mc2, toTypeName(mc2) FROM 03034_nullable_map ORDER BY a;
WITH 'k2'::Nullable(String) AS k SELECT mapContains(m1, k) AS mc1, toTypeName(mc1), mapContains(m2, k) AS mc2, toTypeName(mc2) FROM 03034_nullable_map ORDER BY a;
SELECT mapContains(m1, NULL) AS mc1, toTypeName(mc1), mapContains(m2, NULL) AS mc2, toTypeName(mc2) FROM 03034_nullable_map ORDER BY a;

SELECT 'mapContainsKeyLike';
WITH 'k1' AS k SELECT mapContainsKeyLike(m1, k) AS mc1, toTypeName(mc1), mapContainsKeyLike(m2, k) AS mc2, toTypeName(mc2) FROM 03034_nullable_map ORDER BY a;
WITH 'k%' AS k SELECT mapContainsKeyLike(m1, k) AS mc1, toTypeName(mc1), mapContainsKeyLike(m2, k) AS mc2, toTypeName(mc2) FROM 03034_nullable_map ORDER BY a;
WITH 'k1'::Nullable(String) AS k SELECT mapContainsKeyLike(m1, k) AS mc1, toTypeName(mc1), mapContainsKeyLike(m2, k) AS mc2, toTypeName(mc2) FROM 03034_nullable_map ORDER BY a;
WITH 'k%'::Nullable(String) AS k SELECT mapContainsKeyLike(m1, k) AS mc1, toTypeName(mc1), mapContainsKeyLike(m2, k) AS mc2, toTypeName(mc2) FROM 03034_nullable_map ORDER BY a;
SELECT mapContainsKeyLike(m1, NULL) AS mc1, toTypeName(mc1), mapContainsKeyLike(m2, NULL) AS mc2, toTypeName(mc2) FROM 03034_nullable_map ORDER BY a;

SELECT 'mapExtractKeyLike';
WITH 'k1' AS k SELECT mapExtractKeyLike(m1, k) AS me1, toTypeName(me1), mapExtractKeyLike(m2, k) AS me2, toTypeName(me2) FROM 03034_nullable_map ORDER BY a;
WITH 'k%' AS k SELECT mapExtractKeyLike(m1, k) AS me1, toTypeName(me1), mapExtractKeyLike(m2, k) AS me2, toTypeName(me2) FROM 03034_nullable_map ORDER BY a;
WITH 'k1'::Nullable(String) AS k SELECT mapExtractKeyLike(m1, k) AS me1, toTypeName(me1), mapExtractKeyLike(m2, k) AS me2, toTypeName(me2) FROM 03034_nullable_map ORDER BY a;
WITH 'k%'::Nullable(String) AS k SELECT mapExtractKeyLike(m1, k) AS me1, toTypeName(me1), mapExtractKeyLike(m2, k) AS me2, toTypeName(me2) FROM 03034_nullable_map ORDER BY a;
SELECT mapExtractKeyLike(m1, NULL) AS me1, toTypeName(me1), mapExtractKeyLike(m2, NULL) AS me2, toTypeName(me2) FROM 03034_nullable_map ORDER BY a;

SELECT 'mapAdd';
WITH map('k1', 1)::Map(String, Int) AS cm1, map('k1', 2)::Map(String, Int) AS cm2 SELECT mapAdd(cm1, cm2) AS ma, toTypeName(ma);
WITH map('k1', 1)::Nullable(Map(String, Int)) AS cm1, map('k1', 2)::Map(String, Int) AS cm2 SELECT mapAdd(cm1, cm2) AS ma, toTypeName(ma);
WITH map('k1', 1)::Map(String, Int) AS cm1, map('k1', 2)::Nullable(Map(String, Int)) AS cm2 SELECT mapAdd(cm1, cm2) AS ma, toTypeName(ma);
WITH map('k1', 1)::Nullable(Map(String, Int)) AS cm1, map('k1', 2)::Nullable(Map(String, Int)) AS cm2 SELECT mapAdd(cm1, cm2) AS ma, toTypeName(ma);
WITH map('k1', 1)::Map(String, Int) AS cm1, map('k1', 2)::Map(String, Int) AS cm2 SELECT mapAdd(cm1, cm2, m1) AS ma, toTypeName(ma) FROM 03034_nullable_map ORDER BY a;
WITH map('k1', 1)::Nullable(Map(String, Int)) AS cm1, map('k1', 2)::Map(String, Int) AS cm2 SELECT mapAdd(cm1, cm2, m1) AS ma, toTypeName(ma) FROM 03034_nullable_map ORDER BY a;
WITH map('k1', 1)::Map(String, Int) AS cm1, map('k1', 2)::Nullable(Map(String, Int)) AS cm2 SELECT mapAdd(cm1, cm2, m1) AS ma, toTypeName(ma) FROM 03034_nullable_map ORDER BY a;
WITH map('k1', 1)::Nullable(Map(String, Int)) AS cm1, map('k1', 2)::Nullable(Map(String, Int)) AS cm2 SELECT mapAdd(cm1, cm2, m1) AS ma, toTypeName(ma) FROM 03034_nullable_map ORDER BY a;

SELECT 'mapSubtract';
WITH map('k1', 1)::Map(String, Int) AS cm1, map('k1', 2)::Map(String, Int) AS cm2 SELECT mapSubtract(cm1, cm2) AS ma, toTypeName(ma);
WITH map('k1', 1)::Nullable(Map(String, Int)) AS cm1, map('k1', 2)::Map(String, Int) AS cm2 SELECT mapSubtract(cm1, cm2) AS ma, toTypeName(ma);
WITH map('k1', 1)::Map(String, Int) AS cm1, map('k1', 2)::Nullable(Map(String, Int)) AS cm2 SELECT mapSubtract(cm1, cm2) AS ma, toTypeName(ma);
WITH map('k1', 1)::Nullable(Map(String, Int)) AS cm1, map('k1', 2)::Nullable(Map(String, Int)) AS cm2 SELECT mapSubtract(cm1, cm2) AS ma, toTypeName(ma);
WITH map('k1', 1)::Map(String, Int) AS cm1, map('k1', 2)::Map(String, Int) AS cm2 SELECT mapSubtract(cm1, cm2, m1) AS ma, toTypeName(ma) FROM 03034_nullable_map ORDER BY a;
WITH map('k1', 1)::Nullable(Map(String, Int)) AS cm1, map('k1', 2)::Map(String, Int) AS cm2 SELECT mapSubtract(cm1, cm2, m1) AS ma, toTypeName(ma) FROM 03034_nullable_map ORDER BY a;
WITH map('k1', 1)::Map(String, Int) AS cm1, map('k1', 2)::Nullable(Map(String, Int)) AS cm2 SELECT mapSubtract(cm1, cm2, m1) AS ma, toTypeName(ma) FROM 03034_nullable_map ORDER BY a;
WITH map('k1', 1)::Nullable(Map(String, Int)) AS cm1, map('k1', 2)::Nullable(Map(String, Int)) AS cm2 SELECT mapSubtract(cm1, cm2, m1) AS ma, toTypeName(ma) FROM 03034_nullable_map ORDER BY a;

SELECT 'mapApply';
SELECT mapApply((k, v) -> (concat(k, '__', k), v * 2), m1) AS ma1, toTypeName(ma1), mapApply((k, v) -> (length(k), mapAdd(v, map('kk1', 1::Int32))), m2) AS ma2, toTypeName(ma2) FROM 03034_nullable_map ORDER BY a;

SELECT 'mapFilter';
SELECT mapFilter((k, v) -> k = 'k1', m1) AS mf1, toTypeName(mf1), mapFilter((k, v) -> length(v) = 0, m2) AS mf2, toTypeName(mf2) FROM 03034_nullable_map ORDER BY a;

SELECT 'mapUpdate';
WITH map('k1', 0, 'k2', 0)::Map(String, Int) AS cm SELECT mapUpdate(m1, cm) AS mu1, toTypeName(mu1), mapUpdate(cm, m1) AS mu2, toTypeName(mu2) FROM 03034_nullable_map ORDER BY a;
WITH map('k1', map('kk2', 0))::Map(String, Map(String, Int)) AS cm SELECT mapUpdate(m2, cm) AS mu1, toTypeName(mu1), mapUpdate(cm, m2) AS mu2, toTypeName(mu2) FROM 03034_nullable_map ORDER BY a;
WITH map('k1', 0, 'k2', 0)::Nullable(Map(String, Int)) AS cm SELECT mapUpdate(m1, cm) AS mu1, toTypeName(mu1), mapUpdate(cm, m1) AS mu2, toTypeName(mu2) FROM 03034_nullable_map ORDER BY a;
WITH map('k1', map('kk2', 0))::Nullable(Map(String, Map(String, Int))) AS cm SELECT mapUpdate(m2, cm) AS mu1, toTypeName(mu1), mapUpdate(cm, m2) AS mu2, toTypeName(mu2) FROM 03034_nullable_map ORDER BY a;
WITH materialize(map('k1', 0, 'k2', 0))::Map(String, Int) AS cm SELECT mapUpdate(m1, cm) AS mu1, toTypeName(mu1), mapUpdate(cm, m1) AS mu2, toTypeName(mu2) FROM 03034_nullable_map ORDER BY a;
WITH materialize(map('k1', map('kk2', 0)))::Map(String, Map(String, Int)) AS cm SELECT mapUpdate(m2, cm) AS mu1, toTypeName(mu1), mapUpdate(cm, m2) AS mu2, toTypeName(mu2) FROM 03034_nullable_map ORDER BY a;
WITH materialize(map('k1', 0, 'k2', 0))::Nullable(Map(String, Int)) AS cm SELECT mapUpdate(m1, cm) AS mu1, toTypeName(mu1), mapUpdate(cm, m1) AS mu2, toTypeName(mu2) FROM 03034_nullable_map ORDER BY a;
WITH materialize(map('k1', map('kk2', 0)))::Nullable(Map(String, Map(String, Int))) AS cm SELECT mapUpdate(m2, cm) AS mu1, toTypeName(mu1), mapUpdate(cm, m2) AS mu2, toTypeName(mu2) FROM 03034_nullable_map ORDER BY a;

DROP TABLE 03034_nullable_map;

CREATE TABLE 03034_nullable_map (a Int, m1 Nullable(Map(Int, Int)) KV) ENGINE = CnchMergeTree ORDER BY a;
INSERT INTO 03034_nullable_map VALUES (0, {}) (1, NULL) (2, {2: 10, 5: 20});

SELECT * FROM 03034_nullable_map ORDER BY a;

SELECT 'mapPopulateSeries';
SELECT mapPopulateSeries(m1) AS mp1, toTypeName(mp1), mapPopulateSeries(m1, 3) AS mp2, toTypeName(mp2) FROM 03034_nullable_map ORDER BY a;

DROP TABLE 03034_nullable_map;
