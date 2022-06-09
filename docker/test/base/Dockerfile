# docker build -t yandex/clickhouse-test-base .
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=11

RUN sed -i 's|http://archive|http://cn.archive|g' /etc/apt/sources.list

ENV http_proxy=http://sys-proxy-rd-relay.byted.org:8118
ENV https_proxy=http://sys-proxy-rd-relay.byted.org:8118
ENV no_proxy=.byted.org

RUN apt-get update \
    && apt-get install ca-certificates lsb-release wget gnupg apt-transport-https \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
        /etc/apt/sources.list

# initial packages
RUN apt-get update \
    && apt-get install \
        bash \
        fakeroot \
        ccache \
        curl \
        software-properties-common \
        --yes --no-install-recommends

# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems
RUN curl -O https://clickhouse-datasets.s3.yandex.net/utils/1/dpkg-deb \
    && chmod +x dpkg-deb \
    && cp dpkg-deb /usr/bin

RUN apt-get update \
    && apt-get install \
        clang-${LLVM_VERSION} \
        debhelper \
        devscripts \
        gdb  \
        git \
        gperf \
        lcov \
        llvm-${LLVM_VERSION} \
        moreutils \
        perl \
        pigz \
        pkg-config \
        tzdata \
        pv \
        vim \
        --yes --no-install-recommends

# Upgrade to gdb 11.2
RUN apt-get update \
    && apt-get install libgmp-dev \
        build-essential \
        --yes --no-install-recommends

RUN wget https://ftp.gnu.org/gnu/gdb/gdb-11.2.tar.gz \
    && tar -xvzf gdb-11.2.tar.gz \
    && cd gdb-11.2 \
    && ./configure && make && make install \
    && cd .. && rm -rf gdb-11.2.tar.gz gdb-11.2

# Sanitizer options for services (clickhouse-server)
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=0 history_size=7'" >> /etc/environment; \
  echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
  echo "MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1'" >> /etc/environment; \
  echo "LSAN_OPTIONS='suppressions=/usr/share/clickhouse-test/config/lsan_suppressions.txt'" >> /etc/environment; \
  ln -s /usr/lib/llvm-${LLVM_VERSION}/bin/llvm-symbolizer /usr/bin/llvm-symbolizer;
# Sanitizer options for current shell (not current, but the one that will be spawned on "docker run")
# (but w/o verbosity for TSAN, otherwise test.reference will not match)
ENV TSAN_OPTIONS='halt_on_error=0 history_size=7'
ENV UBSAN_OPTIONS='print_stacktrace=1'
ENV MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1'

# unset http proxy env
ENV http_proxy=
ENV https_proxy=
ENV no_proxy=

# install bvc && sd
RUN mkdir  -p /opt/tiger/bvc && wget -q -O /opt/tiger/bvc/bvc.tar.gz http://d.scm.byted.org/api/v2/download/ceph:tao.modules.bvc_1.0.0.234.tar.gz \
    && tar zxf /opt/tiger/bvc/bvc.tar.gz  --directory /opt/tiger/bvc \
    && ./opt/tiger/bvc/bin/bvc clone -f consul_deploy /opt/tiger/consul_deploy

# install hdfs
RUN YARN_DEPLOY_SCM_REPO=inf/hdfs/cfs_client_volce_deploy_bytehouse && \
    YARN_DEPLOY_SCM_VERSION=1.0.0.3 && \
    JDK_SCM_VERSION=1.0.0.12 && \
    mkdir -p /opt/tiger/yarn_deploy && \
    curl http://d.scm.byted.org/api/v2/download/ceph:$(echo ${YARN_DEPLOY_SCM_REPO} | sed "s/\//./g")_${YARN_DEPLOY_SCM_VERSION}.tar.gz | pigz -dc | tar -x -C /opt/tiger/yarn_deploy  && rm -rf /opt/tiger/yarn_deploy/hadoop/conf/*  && \

    mkdir -p /opt/tiger/jdk && \
    curl http://d.scm.byted.org/api/v2/download/ceph:jdk_${JDK_SCM_VERSION}.tar.gz | pigz -dc | tar -x -C /opt/tiger/jdk  &&  \
    rm -rf /opt/tiger/yarn_deploy/hadoop-2.6.0-cdh5.4.4/libexec/make-config.sh

# env for sd and hdfs
ENV PATH=$PATH:/opt/tiger/yarn_deploy/hadoop/bin:/opt/tiger/consul_deploy/bin/go \
    JAVA_HOME='/opt/tiger/jdk/jdk8u265-b01' \
    HADOOP_HOME='/opt/tiger/yarn_deploy/hadoop' \
    CONSUL_HTTP_HOST=10.8.156.215 \
    CONSUL_HTTP_PORT=2280

CMD sleep 1
