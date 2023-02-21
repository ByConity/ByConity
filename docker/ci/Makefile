pull:
	docker pull byconity/byconity-ci

image:
	test -f fdbmonitor || curl -L -o fdbmonitor https://github.com/apple/foundationdb/releases/download/7.1.0/fdbmonitor.x86_64
	test -f fdbcli || curl -L -o fdbcli https://github.com/apple/foundationdb/releases/download/7.1.0/fdbcli.x86_64
	test -f fdbserver || curl -L -o fdbserver https://github.com/apple/foundationdb/releases/download/7.1.0/fdbserver.x86_64
	test -f hadoop-3.3.4.tar.gz || curl -L -o hadoop-3.3.4.tar.gz https://downloads.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
	docker build --network=host -t byconity/byconity-ci .

image_push:
	docker push byconity/byconity-ci
