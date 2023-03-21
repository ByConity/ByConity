FROM debian:11.6-slim

RUN groupadd -r clickhouse --gid=101 \
&& useradd -r -g clickhouse --uid=101 --home-dir=/var/lib/clickhouse --shell=/bin/bash clickhouse

RUN mkdir -p /root/app/usr/bin \
    && mkdir -p /root/app/logs && mkdir /root/app/data \
    mkdir -p /root/app/config

COPY --chown=clickhouse:clickhouse run.sh /root/app
COPY --chown=clickhouse:clickhouse clickhouse /root/app/usr/bin
COPY --chown=clickhouse:clickhouse foundationdb-clients_7.1.27-1_amd64.deb /root/app 
RUN dpkg -i /root/app/foundationdb-clients_7.1.27-1_amd64.deb
RUN chmod +x /root/app/run.sh

ENTRYPOINT ["/root/app/run.sh"]
