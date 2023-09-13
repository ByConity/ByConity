FROM byconity/debian-builder

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
    openssh-server zsh \
    && rm -rf /var/lib/apt/lists/*

RUN echo "root:root" | chpasswd && \
    sed -ri 's/^#?PermitRootLogin\s+.*/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    echo "port 2222" >> /etc/ssh/sshd_config && \
    mkdir /var/run/sshd

WORKDIR /root/ByConity
ENV CCACHE_DIR=/root/.ccache

EXPOSE 2222
ENTRYPOINT ["/usr/sbin/sshd", "-D"]
