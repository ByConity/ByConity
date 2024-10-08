DROP TABLE IF EXISTS ndcg_test;
CREATE TABLE ndcg_test (rank_id Int64, rank Int16, pred Float64, label Float64) engine=CnchMergeTree order by tuple();
INSERT INTO ndcg_test values(1, 1, 1, 1);

select ndcg(rank_id, rank, pred, label) from ndcg_test;

DROP TABLE ndcg_test;