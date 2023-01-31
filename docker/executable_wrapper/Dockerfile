FROM debian:latest

RUN groupadd -r clickhouse --gid=101 \
&& useradd -r -g clickhouse --uid=101 --home-dir=/var/lib/clickhouse --shell=/bin/bash clickhouse

RUN mkdir -p /root/app/usr/bin && mkdir /root/app/lib \
    && mkdir -p /root/app/logs && mkdir /root/app/data \
    mkdir -p /root/app/config
ENV LD_LIBRARY_PATH=/root/app/lib

COPY --chown=clickhouse:clickhouse run.sh /root/app
COPY --chown=clickhouse:clickhouse clickhouse /root/app/usr/bin
COPY --chown=clickhouse:clickhouse libfdb_c.so /root/app/lib
RUN chmod +x /root/app/run.sh

ENTRYPOINT ["/root/app/run.sh"]
