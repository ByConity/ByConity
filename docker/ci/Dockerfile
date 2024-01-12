FROM byconity/byconity-builder
COPY fdbmonitor fdbcli fdbserver /opt/tiger/foundationdb/bin/
RUN chmod +x /opt/tiger/foundationdb/bin/*
COPY docker-compose /usr/local/bin/docker-compose
RUN chmod +x /usr/local/bin/docker-compose
RUN pip install requests packaging

# Install docker and docker-compose
RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
RUN chmod a+r /etc/apt/keyrings/docker.gpg
RUN echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

COPY hadoop-3.3.4.tar.gz / 
ADD install.sh /data/ci/
RUN bash /data/ci/install.sh
RUN curl https://clickhouse.com/ | sh
RUN apt-get install -y gdb
RUN mkdir /docker_image_archive
COPY preload_gchq-hdfs_3_3.tar preload_foundationdb-foundationdb_7_1_24.tar preload_byconity-debian_buster_runit_fdb7_1_27.tar /docker_image_archive/
