set enable_ab_test=1;
set ab_test_traffic_factor=1;
set ab_test_profile='test_optimizer_ab_test';

explain select * from system.one;

select host() AS host from clusterAllReplicas(test_cluster_two_shards, system, one) format Null;
