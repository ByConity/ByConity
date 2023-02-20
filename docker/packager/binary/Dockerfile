# rebuild in #33610
# docker build -t byconity/binary-builder .
FROM minhthucdao1/byconity-builder
RUN apt-get install -y curl moreutils
ENV CC=clang-11
ENV CXX=clang++-11

# Architecture of the image when BuildKit/buildx is used
ARG TARGETARCH
ARG NFPM_VERSION=2.20.0

RUN arch=${TARGETARCH:-amd64} \
  && curl -Lo /tmp/nfpm.deb "https://github.com/goreleaser/nfpm/releases/download/v${NFPM_VERSION}/nfpm_${arch}.deb" \
  && dpkg -i /tmp/nfpm.deb \
  && rm /tmp/nfpm.deb

RUN mkdir /workdir && chmod 777 /workdir
WORKDIR /workdir

COPY build.sh /
CMD ["bash", "-c", "/build.sh 2>&1"]
