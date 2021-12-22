# docker build -t yandex/clickhouse-stateful-test .
ARG TAG=stable
FROM hub.byted.org/bytehouse/clickhouse-test-base:${TAG}

# set http proxy env
ENV http_proxy=http://sys-proxy-rd-relay.byted.org:8118
ENV https_proxy=http://sys-proxy-rd-relay.byted.org:8118
ENV no_proxy=.byted.org

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python3-requests \
        llvm-9

COPY s3downloader /s3downloader

ENV DATASETS="hits visits"

# unset http proxy env
ENV http_proxy=
ENV https_proxy=
ENV no_proxy=

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
