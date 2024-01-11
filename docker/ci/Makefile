pull:
	docker pull byconity/byconity-ci

image:
	test -f fdbmonitor || curl -L -o fdbmonitor https://github.com/apple/foundationdb/releases/download/7.1.0/fdbmonitor.x86_64
	test -f fdbcli || curl -L -o fdbcli https://github.com/apple/foundationdb/releases/download/7.1.0/fdbcli.x86_64
	test -f fdbserver || curl -L -o fdbserver https://github.com/apple/foundationdb/releases/download/7.1.0/fdbserver.x86_64
	test -f hadoop-3.3.4.tar.gz || curl -L -o hadoop-3.3.4.tar.gz https://downloads.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
	test -f docker-compose || curl -L -o docker-compose "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-Linux-x86_64"
	docker pull gchq/hdfs:3.3
	docker pull foundationdb/foundationdb:7.1.24
	docker pull byconity/debian:buster-runit-fdb7.1.27
	test -f preload_gchq-hdfs_3_3.tar || docker image save --output preload_gchq-hdfs_3_3.tar gchq/hdfs:3.3
	test -f preload_foundationdb-foundationdb_7_1_24.tar || docker image save --output preload_foundationdb-foundationdb_7_1_24.tar foundationdb/foundationdb:7.1.24
	test -f preload_byconity-debian_buster_runit_fdb7_1_27.tar || docker image save --output preload_byconity-debian_buster_runit_fdb7_1_27.tar byconity/debian:buster-runit-fdb7.1.27
	docker build --network=host -t byconity/byconity-ci .

image_push:
	docker push byconity/byconity-ci
image_test:
	docker run --rm -it byconity/byconity-ci /bin/bash
