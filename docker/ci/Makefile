pull:
	docker pull byconity/byconity-ci:llvm16

image:
	test -f fdbmonitor || curl -L -o fdbmonitor https://github.com/apple/foundationdb/releases/download/7.1.0/fdbmonitor.x86_64
	test -f fdbcli || curl -L -o fdbcli https://github.com/apple/foundationdb/releases/download/7.1.0/fdbcli.x86_64
	test -f fdbserver || curl -L -o fdbserver https://github.com/apple/foundationdb/releases/download/7.1.0/fdbserver.x86_64
	test -f hadoop-3.3.4.tar.gz || curl -L -o hadoop-3.3.4.tar.gz https://downloads.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
	test -f containerd.io_1.6.26-1_amd64.deb || curl -L -o containerd.io_1.6.26-1_amd64.deb "https://download.docker.com/linux/debian/dists/bullseye/pool/stable/amd64/containerd.io_1.6.26-1_amd64.deb"
	test -f docker-ce-cli_24.0.7-1~debian.11~bullseye_amd64.deb || curl -L -o docker-ce-cli_24.0.7-1~debian.11~bullseye_amd64.deb "https://download.docker.com/linux/debian/dists/bullseye/pool/stable/amd64/docker-ce-cli_24.0.7-1~debian.11~bullseye_amd64.deb"
	test -f docker-ce_24.0.7-1~debian.11~bullseye_amd64.deb || curl -L -o docker-ce_24.0.7-1~debian.11~bullseye_amd64.deb "https://download.docker.com/linux/debian/dists/bullseye/pool/stable/amd64/docker-ce_24.0.7-1~debian.11~bullseye_amd64.deb"
	test -f docker-compose-plugin_2.21.0-1~debian.11~bullseye_amd64.deb || curl -L -o docker-compose-plugin_2.21.0-1~debian.11~bullseye_amd64.deb "https://download.docker.com/linux/debian/dists/bullseye/pool/stable/amd64/docker-compose-plugin_2.21.0-1~debian.11~bullseye_amd64.deb"
	test -f docker-buildx-plugin_0.11.2-1~debian.11~bullseye_amd64.deb || curl -L -o docker-buildx-plugin_0.11.2-1~debian.11~bullseye_amd64.deb "https://download.docker.com/linux/debian/dists/bullseye/pool/stable/amd64/docker-buildx-plugin_0.11.2-1~debian.11~bullseye_amd64.deb"
	test -f docker-ce-rootless-extras_24.0.7-1~debian.11~bullseye_amd64.deb || curl -L -o docker-ce-rootless-extras_24.0.7-1~debian.11~bullseye_amd64.deb "https://download.docker.com/linux/debian/dists/bullseye/pool/stable/amd64/docker-ce-rootless-extras_24.0.7-1~debian.11~bullseye_amd64.deb"
	docker pull gchq/hdfs:3.3
	docker pull foundationdb/foundationdb:7.1.24
	docker pull byconity/debian:buster-runit-fdb7.1.27
	test -f preload_gchq-hdfs_3_3.tar || docker image save --output preload_gchq-hdfs_3_3.tar gchq/hdfs:3.3
	test -f preload_foundationdb-foundationdb_7_1_24.tar || docker image save --output preload_foundationdb-foundationdb_7_1_24.tar foundationdb/foundationdb:7.1.24
	test -f preload_byconity-debian_buster_runit_fdb7_1_27.tar || docker image save --output preload_byconity-debian_buster_runit_fdb7_1_27.tar byconity/debian:buster-runit-fdb7.1.27
	test -f preload_minio-minio_RELEASE.2024-01-11T07-46-16Z.tar || docker image save --output preload_minio-minio_RELEASE.2024-01-11T07-46-16Z.tar minio/minio:RELEASE.2024-01-11T07-46-16Z
	test -f preload_minio-mc_RELEASE.2024-01-11T05-49-32Z.tar || docker image save --output preload_minio-mc_RELEASE.2024-01-11T05-49-32Z.tar minio/mc:RELEASE.2024-01-11T05-49-32Z
	docker build --network=host -t byconity/byconity-ci:llvm16 .

image_push:
	docker push byconity/byconity-ci:llvm16
image_test:
	docker run --rm -it byconity/byconity-ci:llvm16 /bin/bash
