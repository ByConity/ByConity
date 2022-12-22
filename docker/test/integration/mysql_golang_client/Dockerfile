# docker build -t yandex/clickhouse-mysql-golang-client .
# MySQL golang client docker container

FROM golang:1.13

ENV http_proxy=http://sys-proxy-rd-relay.byted.org:8118
ENV https_proxy=http://sys-proxy-rd-relay.byted.org:8118
ENV no_proxy=.byted.org

RUN go get "github.com/go-sql-driver/mysql"

# unset http proxy env
ENV http_proxy=
ENV https_proxy=
ENV no_proxy=

COPY ./main.go main.go

RUN go build main.go
