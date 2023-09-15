FROM byconity/debian-runner

ARG APP_ROOT=/opt/byconity
COPY build_install ${APP_ROOT}
WORKDIR ${APP_ROOT}

ENV PATH="${PATH}:${APP_ROOT}/bin" \
    LD_LIBRARY_PATH="${APP_ROOT}/lib:${LD_LIBRARY_PATH}" \
    APP_ROOT="${APP_ROOT}"

RUN mkdir -p /etc/service/byconity
COPY run /etc/service/
