# docker build -t yandex/clickhouse-stateful-test .
ARG TAG=stable
FROM hub.byted.org/bytehouse/clickhouse-stateless-test:${TAG}

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

# prepare statefull dataset
RUN git clone --depth 1 https://gitlab+deploy-token-3553:pcgsySXCwK8md2BHebPw@code.byted.org/dp/ClickHouse-ci-dataset.git

ENV DATASETS="hits visits"

# unset http proxy env
ENV http_proxy=
ENV https_proxy=
ENV no_proxy=

# COPY run.sh /
# CMD ["/bin/bash", "/run.sh"]
