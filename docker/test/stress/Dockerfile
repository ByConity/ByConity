# docker build -t yandex/clickhouse-stress-test .
ARG TAG=stable
FROM hub.byted.org/bytehouse/clickhouse-stateful-test:${TAG}

# set http proxy env
ENV http_proxy=http://sys-proxy-rd-relay.byted.org:8118
ENV https_proxy=http://sys-proxy-rd-relay.byted.org:8118
ENV no_proxy=.byted.org

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            tzdata \
            fakeroot \
            debhelper \
            parallel \
            expect \
            python3 \
            python3-lxml \
            python3-termcolor \
            python3-requests \
            curl \
            sudo \
            openssl \
            netcat-openbsd \
            telnet \
            llvm-9 \
            brotli

ENV http_proxy=
ENV https_proxy=
ENV no_proxy=

COPY ./stress /stress
COPY run.sh /

ENV DATASETS="hits visits"
#CMD ["/bin/bash", "/run.sh"]
