# Docker image intended for local development using vscode/zed
FROM mcr.microsoft.com/devcontainers/rust:2-1-trixie

# Download the package to configure the Microsoft repo
RUN curl -sSL -O https://packages.microsoft.com/config/debian/13/packages-microsoft-prod.deb
# Install the package
RUN dpkg -i packages-microsoft-prod.deb
RUN rm packages-microsoft-prod.deb

# Install packages
RUN echo msodbcsql18 msodbcsql/ACCEPT_EULA boolean true | debconf-set-selections
RUN apt update && export DEBIAN_FRONTEND=noninteractive \
&& apt-get -y install --no-install-recommends \
    build-essential \
    unixodbc-dev \
    msodbcsql18 \
    odbc-postgresql \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Setup cargo and install cargo packages required for tests
USER vscode

RUN cargo install parquet --features cli
