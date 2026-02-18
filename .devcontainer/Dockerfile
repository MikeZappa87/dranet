FROM mcr.microsoft.com/devcontainers/go:1.25

RUN apt-get update && apt-get install -y --no-install-recommends \
    kmod \
    iproute2 \
    iputils-ping \
    ethtool \
    tcpdump \
    jq \
    ca-certificates \
    bash-completion \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /work
