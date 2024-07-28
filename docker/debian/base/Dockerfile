# Download stage
FROM alpine as downloader
ARG FDB_VERSION=7.1.27
WORKDIR /downloads
RUN apk --no-cache add wget
RUN wget -qO cmake.3.17.tar.gz https://cmake.org/files/v3.17/cmake-3.17.0-Linux-x86_64.tar.gz
# RUN wget https://apt.llvm.org/llvm.sh
RUN wget -q https://mirrors.tuna.tsinghua.edu.cn/llvm-apt/llvm.sh
RUN wget -qO foundationdb-clients.deb https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb
RUN wget -qO tini https://github.com/krallin/tini/releases/download/v0.19.0/tini

# Debian builder
FROM debian:11.9 as debian-builder
LABEL description="Debian image for compiling"
LABEL org.opencontainers.image.source="https://github.com/ByConity/ByConity"

RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
    binutils gettext-base strace bpftrace sysstat gdb maven default-jdk ninja-build \
    lsb-release wget curl software-properties-common gnupg ccache git vim zlib1g-dev \
    autoconf automake m4 bison flex \
    && rm -rf /var/lib/apt/lists/*

COPY --from=downloader /downloads /downloads
WORKDIR /downloads
RUN tar --strip-components=1 -xzf cmake.3.17.tar.gz -C /usr/local
RUN chmod +x llvm.sh && ./llvm.sh 11 -m https://mirrors.tuna.tsinghua.edu.cn/llvm-apt
RUN dpkg -i foundationdb-clients.deb
RUN rm -rf /downloads

COPY build.sh /
RUN chmod +x /build.sh

ENV JAVA_HOME=/usr/lib/jvm/default-java
RUN echo "/usr/lib/jvm/default-java/lib" >> /etc/ld.so.conf.d/libjvm.conf
RUN echo "/usr/lib/jvm/default-java/lib/server" >> /etc/ld.so.conf.d/libjvm.conf
RUN ldconfig

ENV CC=clang-11 CXX=clang++-11

# Base runner image
FROM debian:11.9-slim as debian-runner
LABEL description="Base Debian image for runtime"
LABEL org.opencontainers.image.source="https://github.com/ByConity/ByConity"

RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
    runit binutils gettext-base strace bpftrace sysstat gdb curl vim nano net-tools linux-perf \
    dstat procps telnet \
    && rm -rf /var/lib/apt/lists/*

COPY --from=downloader /downloads /downloads
WORKDIR /downloads
RUN dpkg -i foundationdb-clients.deb
RUN mv /downloads/tini /tini && chmod +x /tini
RUN rm -rf /downloads

# /opt/local/bin for attaching local compiled binary, see docker-compose
ENV PATH="${PATH}:/opt/local/bin"

# APP_ROOT is the install dir
ARG APP_ROOT=/opt/byconity
ENV PATH="${PATH}:${APP_ROOT}/bin" \
    LD_LIBRARY_PATH="${APP_ROOT}/lib:${LD_LIBRARY_PATH}" \
    APP_ROOT="${APP_ROOT}"

WORKDIR /
ENTRYPOINT ["/tini", "--"]
CMD ["runsvdir", "/etc/service"]
