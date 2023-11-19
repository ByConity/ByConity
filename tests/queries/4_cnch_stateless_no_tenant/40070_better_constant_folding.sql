SET enable_optimizer = 1;

-- { echo }
EXPLAIN SELECT range(5);
EXPLAIN SELECT range(5000);
EXPLAIN SELECT length(range(5000));
EXPLAIN SELECT range(2000 + 3000);
