DROP TABLE IF EXISTS multi_if_like_rewrite;

CREATE TABLE multi_if_like_rewrite(`c1` Nullable(String), `c2` Nullable(String)) ENGINE = CnchMergeTree() order by tuple() partition by tuple();

INSERT INTO multi_if_like_rewrite values ('KoreaJapan0','Japan0')('Korea1','Korea1')('Japan1','Japan1')('SEA1','SEA1')('other','Korea2')('other','Japan2')('other', 'SEA2')(NULL,'Japan3');

SELECT c2 FROM multi_if_like_rewrite
WHERE CASE
          when c1 like '%Korea%' then 'Korea'
          when c1 like '%Japan%' then 'Japan'
          when c1 like '%SEA%' then 'SEA'
          when c2 like '%Korea%' then 'Korea'
          when c2 like '%Japan%' then 'Japan'
          when c2 like '%SEA%' then 'SEA'
          else 'other'
          END = 'Japan'
ORDER BY c2;

SELECT c2 FROM multi_if_like_rewrite
WHERE (
        isNull(c1)
        OR NOT multiSearchAny(c1, ['Korea'])
    )
  AND (
        (c1 LIKE '%Japan%')
        OR (
                (
                        isNull(c1)
                        OR NOT multiSearchAny(c1, ['Japan', 'SEA'])
                    )
                AND (
                        isNull(c2)
                        OR NOT multiSearchAny(c2, ['Korea'])
                    )
                AND (c2 LIKE '%Japan%')
            )
    )
ORDER BY c2;

DROP TABLE IF EXISTS multi_if_like_rewrite;