# docker build -t yandex/clickhouse-stateful-test .
ARG TAG=stable
FROM bytehouse/clickhouse-stateless-test:${TAG}

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python3-requests \
        llvm-9

COPY s3downloader /s3downloader

# COPY run.sh /
# CMD ["/bin/bash", "/run.sh"]
