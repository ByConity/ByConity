import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1")
node2 = cluster.add_instance("node2", main_configs=["configs/config_with_standard_part_log.xml"])
node3 = cluster.add_instance("node3", main_configs=["configs/config_with_non_standard_part_log.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_config_without_part_log(start_cluster):
    assert "Table system.part_log doesn't exist" in node1.query_and_get_error("SELECT * FROM system.part_log")
    node1.query("CREATE TABLE test_table(word String, value UInt64) ENGINE=MergeTree() ORDER BY value")
    assert "Table system.part_log doesn't exist" in node1.query_and_get_error("SELECT * FROM system.part_log")
    node1.query("INSERT INTO test_table VALUES ('name', 1)")
    node1.query("SYSTEM FLUSH LOGS")
    assert "Table system.part_log doesn't exist" in node1.query_and_get_error("SELECT * FROM system.part_log")


# Note: if part_log is defined, we cannot say when the table will be created - because of metric_log, trace_log, text_log, query_log...

def test_config_with_standard_part_log(start_cluster):
    node2.query("CREATE TABLE test_table(word String, value UInt64) ENGINE=MergeTree() Order by value")
    node2.query("INSERT INTO test_table VALUES ('name', 1)")
    node2.query("SYSTEM FLUSH LOGS")
    assert node2.query("SELECT * FROM system.part_log") != ""

def test_part_log_contains_partition(start_cluster):
    node2.query(
        "CREATE TABLE test_partition_table (date Date, word String, value UInt64) ENGINE=MergeTree() "
        + "PARTITION BY toYYYYMM(date) Order by value"
    )
    node2.query(
        "INSERT INTO test_partition_table VALUES "
        + "('2023-06-20', 'a', 10), ('2023-06-21', 'b', 11),"
        + "('2023-05-20', 'cc', 14),('2023-05-21', 'd1', 15);"
    )
    node2.query("SYSTEM FLUSH LOGS")
    resp = node2.query(
        "SELECT partition from system.part_log where table = 'test_partition_table'"
    )
    assert resp == "202306\n202305\n"

def test_config_with_non_standard_part_log(start_cluster):
    node3.query("CREATE TABLE test_table(word String, value UInt64) ENGINE=MergeTree() Order by value")
    node3.query("INSERT INTO test_table VALUES ('name', 1)")
    node3.query("SYSTEM FLUSH LOGS")
    assert node3.query("SELECT * FROM system.own_part_log") != ""
