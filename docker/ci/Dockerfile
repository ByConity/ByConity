FROM byconity/byconity-builder
COPY fdbmonitor fdbcli fdbserver /opt/tiger/foundationdb/bin/
RUN chmod +x /opt/tiger/foundationdb/bin/*
RUN pip install requests

COPY hadoop-3.3.4.tar.gz / 
ADD install.sh /data/ci/
RUN bash /data/ci/install.sh
