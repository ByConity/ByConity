FROM byconity/byconity-builder
COPY fdbmonitor fdbcli fdbserver /opt/tiger/foundationdb/bin/
RUN chmod +x /opt/tiger/foundationdb/bin/*
RUN pip install requests

# Install docker and docker-compose
RUN install -m 0755 -d /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
RUN chmod a+r /etc/apt/keyrings/docker.gpg
RUN echo "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update
RUN apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
RUN curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
RUN chmod +x /usr/local/bin/docker-compose

COPY hadoop-3.3.4.tar.gz / 
ADD install.sh /data/ci/
RUN bash /data/ci/install.sh
RUN curl https://clickhouse.com/ | sh
RUN apt-get install -y gdb
