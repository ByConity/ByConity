SET enable_optimizer = 1;

-- { echo }
EXPLAIN SELECT range(5);
EXPLAIN SELECT range(5000);
EXPLAIN SELECT length(range(5000));
EXPLAIN SELECT range(2000 + 3000);
EXPLAIN SELECT mapFromArrays(range(5), range(5));
EXPLAIN SELECT mapFromArrays(range(5000), range(5000));
EXPLAIN SELECT initializeAggregation('sumState', 1);
EXPLAIN SELECT map(1, initializeAggregation('sumState', 1));
