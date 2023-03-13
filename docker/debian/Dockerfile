FROM byconity/debian:buster-runit

# Install foudationdb lib and cli
RUN wget https://github.com/apple/foundationdb/releases/download/7.1.27/fdbcli.x86_64 -O /usr/bin/fdbcli
RUN wget https://github.com/apple/foundationdb/releases/download/7.1.27/libfdb_c.x86_64.so -O /usr/lib/libfdb_c.so
RUN chmod +x /usr/bin/fdbcli
RUN chmod +x /usr/lib/libfdb_c.so

# Install runit

RUN rm -rf /var/lib/apt/lists/* /var/cache/apt/* /tmp/* /var/tmp/*
