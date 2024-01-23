#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "CapnProto"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format CapnProto settings format_schema='$CURDIR/format_schemas/02566_ipv4_ipv6:Message'" > 02566_ipv4_ipv6_data.capnp
${CLICKHOUSE_LOCAL} -q "select * from file(02566_ipv4_ipv6_data.capnp, 'CapnProto', 'ipv6 IPv6, ipv4 IPv4') settings format_schema='$CURDIR/format_schemas/02566_ipv4_ipv6:Message'"
rm 02566_ipv4_ipv6_data.capnp

echo "Avro"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format Avro"  > 02566_ipv4_ipv6_data.avro
${CLICKHOUSE_LOCAL} -q "select * from file(02566_ipv4_ipv6_data.avro, 'Avro', 'ipv6 IPv6, ipv4 IPv4')"
rm 02566_ipv4_ipv6_data.avro

echo "Arrow"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format Arrow"  > 02566_ipv4_ipv6_data.arrow
${CLICKHOUSE_LOCAL} -q "select * from file(02566_ipv4_ipv6_data.arrow, 'Arrow', 'ipv6 IPv6, ipv4 IPv4')"
rm 02566_ipv4_ipv6_data.arrow

echo "Parquet"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format Parquet"  > 02566_ipv4_ipv6_data.parquet
${CLICKHOUSE_LOCAL} -q "select ipv6, toIPv4(ipv4) from file(02566_ipv4_ipv6_data.parquet, 'Parquet', 'ipv6 IPv6, ipv4 UInt32')"
rm 02566_ipv4_ipv6_data.parquet

echo "MsgPack"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format MsgPack"  > 02566_ipv4_ipv6_data.msgpack
${CLICKHOUSE_LOCAL} -q "select * from file(02566_ipv4_ipv6_data.msgpack, 'MsgPack', 'ipv6 IPv6, ipv4 IPv4')"
rm 02566_ipv4_ipv6_data.msgpack


