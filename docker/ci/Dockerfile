FROM byconity/byconity-builder:llvm16
RUN pip install requests packaging && apt-get install -y gdb 
# Install docker dependencies
RUN apt-get install -y apparmor libltdl7 pigz
COPY fdbmonitor fdbcli fdbserver /opt/tiger/foundationdb/bin/
RUN chmod +x /opt/tiger/foundationdb/bin/*

# Install docker and docker-compose
COPY containerd.io_1.6.26-1_amd64.deb docker-ce-cli_24.0.7-1~debian.11~bullseye_amd64.deb docker-ce_24.0.7-1~debian.11~bullseye_amd64.deb docker-compose-plugin_2.21.0-1~debian.11~bullseye_amd64.deb docker-buildx-plugin_0.11.2-1~debian.11~bullseye_amd64.deb docker-ce-rootless-extras_24.0.7-1~debian.11~bullseye_amd64.deb /
RUN apt install -y /docker-ce-rootless-extras_24.0.7-1~debian.11~bullseye_amd64.deb \
&& apt install -y /docker-ce-cli_24.0.7-1~debian.11~bullseye_amd64.deb \
&& apt install -y /containerd.io_1.6.26-1_amd64.deb \
&& apt install -y /docker-ce_24.0.7-1~debian.11~bullseye_amd64.deb \
&& apt install -y /docker-buildx-plugin_0.11.2-1~debian.11~bullseye_amd64.deb \
&& apt install -y /docker-compose-plugin_2.21.0-1~debian.11~bullseye_amd64.deb

COPY hadoop-3.3.4.tar.gz / 
ADD install.sh /data/ci/
RUN bash /data/ci/install.sh
RUN curl https://clickhouse.com/ | sh
RUN mkdir /docker_image_archive
COPY preload_gchq-hdfs_3_3.tar preload_foundationdb-foundationdb_7_1_24.tar preload_byconity-debian_buster_runit_fdb7_1_27.tar preload_minio-minio_RELEASE.2024-01-11T07-46-16Z.tar preload_minio-mc_RELEASE.2024-01-11T05-49-32Z.tar /docker_image_archive/
