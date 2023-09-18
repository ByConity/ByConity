FROM byconity/debian-runner

ARG APP_ROOT=/opt/byconity
COPY build_install ${APP_ROOT}
WORKDIR ${APP_ROOT}

ENV PATH="${PATH}:${APP_ROOT}/bin" \
    LD_LIBRARY_PATH="${APP_ROOT}/lib:${LD_LIBRARY_PATH}" \
    APP_ROOT="${APP_ROOT}"

# JRE
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
    default-jre \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
RUN echo "/usr/lib/jvm/default-java/lib" >> /etc/ld.so.conf.d/libjvm.conf
RUN echo "/usr/lib/jvm/default-java/lib/server" >> /etc/ld.so.conf.d/libjvm.conf
RUN ldconfig
ENV CLASSPATH=${CLASSPATH}:${APP_ROOT}/bin/

# config runit, see base image
RUN mkdir -p /etc/service/byconity
COPY run /etc/service/byconity/run
RUN chmod 755 /etc/service/byconity/run
