SELECT 1 FORMAT Null SETTINGS enable_optimizer=0, print_graphviz=1, log_queries=1, log_queries_min_type='QUERY_FINISH';
SYSTEM FLUSH LOGS;

WITH 
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() 
        AND Settings['enable_optimizer']='0'
        AND Settings['print_graphviz']='1'
        ORDER BY event_time desc LIMIT 1
    ) AS query_id_
SELECT
    hasAll(Graphviz.Names, ['5000_pipeline-grouped_0_coordinator','5000_pipeline_0_coordinator'])
FROM system.query_log
WHERE query_id = query_id_
LIMIT 1;

SELECT 1 FORMAT Null SETTINGS enable_optimizer=0, print_graphviz=0, log_queries=1, log_queries_min_type='QUERY_FINISH';
SYSTEM FLUSH LOGS;

WITH 
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() 
        AND Settings['enable_optimizer']='0'
        AND Settings['print_graphviz']='0'
        ORDER BY event_time desc LIMIT 1
    ) AS query_id_
SELECT
    Graphviz.Names as names
FROM system.query_log
WHERE query_id = query_id_
LIMIT 1;


SELECT 1 FORMAT Null SETTINGS enable_optimizer=1, print_graphviz=1, log_queries=1, log_queries_min_type='QUERY_FINISH';
SYSTEM FLUSH LOGS;

WITH 
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() 
        AND Settings['enable_optimizer']='1'
        AND Settings['print_graphviz']='1'
        ORDER BY event_time desc LIMIT 1
    ) AS query_id_
SELECT
    hasAll(Graphviz.Names, ['1000-AST-init','3000_Init_Plan','4000-PlanSegment','5000_pipeline-grouped_0_coordinator','5000_pipeline_0_coordinator'])
FROM system.query_log
WHERE query_id = query_id_
LIMIT 1;

SELECT 1 FORMAT Null SETTINGS enable_optimizer=1, print_graphviz=0, log_queries=1, log_queries_min_type='QUERY_FINISH';
SYSTEM FLUSH LOGS;

WITH 
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() 
        AND Settings['enable_optimizer']='1'
        AND Settings['print_graphviz']='0'
        ORDER BY event_time desc LIMIT 1
    ) AS query_id_
SELECT
    Graphviz.Names as names
FROM system.query_log
WHERE query_id = query_id_
LIMIT 1;