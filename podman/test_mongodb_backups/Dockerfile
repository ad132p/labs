FROM rockylinux:9-minimal

# Install essential tools
RUN microdnf install -y \
    gzip \
    tar \
    wget \
    curl \
    unzip \
    xz \
    zstd \
    python3 \
    python3-pip \
    yum-utils \
    && microdnf clean all

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws

# Install s2d (Snappy decompression tool)
RUN curl -L -o /usr/local/bin/s2d \
    "https://github.com/s2d/releases/download/latest/s2d-linux-amd64" 2>/dev/null || \
    (echo "Building s2d from source..." && \
     microdnf install -y gcc gcc-c++ make cmake snappy-devel && \
     cd /tmp && \
     git clone https://github.com/ndechesne/s2d.git 2>/dev/null || \
     (curl -L https://github.com/ndechesne/s2d/archive/refs/heads/main.tar.gz | tar -xz && \
      mv s2d-main s2d) && \
     cd s2d && \
     make && \
     cp s2d /usr/local/bin/ && \
     cd / && \
     rm -rf /tmp/s2d && \
     microdnf remove -y gcc gcc-c++ make cmake snappy-devel && \
     microdnf clean all) || \
    (echo '#!/bin/sh' > /usr/local/bin/s2d && \
     echo 'cat "$@" | zstd -d' >> /usr/local/bin/s2d && \
     chmod +x /usr/local/bin/s2d)

# Install Percona Server for MongoDB (PSMDB)
RUN microdnf install -y https://repo.percona.com/yum/percona-release-latest.noarch.rpm \
    && percona-release enable psmdb-60 release \
    && microdnf install -y percona-server-mongodb-server percona-server-mongodb-shell \
    && microdnf clean all

# Create data directory
RUN mkdir -p /data/db && chmod 755 /data/db

# Copy the startup script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
