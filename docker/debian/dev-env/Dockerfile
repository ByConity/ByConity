FROM byconity/debian-builder

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
    build-essential openssh-server make zsh netcat vim wget \
    && rm -rf /var/lib/apt/lists/*

RUN echo "root:root" | chpasswd && \
    sed -ri 's/^#?PermitRootLogin\s+.*/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    echo "port 2222" >> /etc/ssh/sshd_config && \
    mkdir /var/run/sshd

WORKDIR /root/ByConity
ENV CCACHE_DIR=/root/.ccache
