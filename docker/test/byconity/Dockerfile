# docker build -t bycontiny/byconity-test --build-arg TAG=<value>
ARG TAG=latest
FROM byconity/byconity:${TAG}
RUN apt-get update && apt-get install -y dnsutils moreutils pip
RUN pip install requests 
RUN mkdir -p /root/app
WORKDIR /root/app
COPY artifacts/clickhouse-test /root/app/clickhouse-test
COPY artifacts/process_functional_tests_result.py /root/app/process_functional_tests_result.py
COPY artifacts/queries /root/app/queries
COPY clean.sh /root/app/clean.sh 
COPY run.sh /root/app/run.sh 

CMD ["/bin/bash", "/root/app/run.sh"]
