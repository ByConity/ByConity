# docker build -t yandex/clickhouse-integration-test .
ARG TAG=stable
FROM hub.byted.org/bytehouse/clickhouse-test-base:${TAG}

SHELL ["/bin/bash", "-c"]

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get -y install \
        tzdata \
        python3 \
        libreadline-dev \
        libicu-dev \
        bsdutils \
        gdb \
        unixodbc \
        odbcinst \
        rpm2cpio \
        libsqlite3-dev \
        libsqliteodbc \
        odbc-postgresql \
        sqlite3 \
        curl \
        tar \
        krb5-user \
        iproute2 \
        lsof \
        g++ \
        default-jre

RUN rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
RUN apt-get clean

# Architecture of the image when BuildKit/buildx is used
ARG TARGETARCH

# Install MySQL ODBC driver from RHEL rpm
RUN arch=${TARGETARCH:-amd64} \
  && case $arch in \
      amd64) rarch=x86_64 ;; \
      arm64) rarch=aarch64 ;; \
    esac \
  && cd /tmp \
  && curl -o mysql-odbc.rpm "https://cdn.mysql.com/Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.27-1.el8.${rarch}.rpm" \
  && rpm2archive mysql-odbc.rpm \
  && tar xf mysql-odbc.rpm.tgz -C / ./usr/lib64/ \
  && LINK_DIR=$(dpkg -L libodbc1 | grep '^/usr/lib/.*-linux-gnu/odbc$') \
  && ln -s /usr/lib64/libmyodbc8a.so "$LINK_DIR" \
  && ln -s /usr/lib64/libmyodbc8a.so "$LINK_DIR"/libmyodbc.so

# Unfortunately this is required for a single test for conversion data from zookeeper to clickhouse-keeper.
# ZooKeeper is not started by default, but consumes some space in containers.
# 777 perms used to allow anybody to start/stop ZooKeeper
ENV ZOOKEEPER_VERSION='3.6.3'
RUN curl -O "https://mirrors.estointernet.in/apache/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"
RUN tar -zxvf apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz && mv apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper && chmod -R 777 /opt/zookeeper && rm apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
RUN echo $'tickTime=2500 \n\
tickTime=2500 \n\
dataDir=/zookeeper \n\
clientPort=2181 \n\
maxClientCnxns=80' > /opt/zookeeper/conf/zoo.cfg
RUN mkdir /zookeeper && chmod -R 777 /zookeeper

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

