# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.163.1/containers/debian/.devcontainer/base.Dockerfile

# [Choice] Debian version: buster, stretch
ARG VARIANT="buster"
FROM mcr.microsoft.com/vscode/devcontainers/base:0-${VARIANT}

# Install Microsoft ODBC SQL Drivers (msodbcsql17 package) for Debian 10
# https://docs.microsoft.com/de-de/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN echo msodbcsql17 msodbcsql/ACCEPT_EULA boolean true | debconf-set-selections

# This section install additional components
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
	&& apt-get -y install --no-install-recommends \
	build-essential \
	unixodbc-dev \
	msodbcsql17 \
	odbc-postgresql

# Fix PostgreSQL driver paths
RUN sed --in-place 's/psqlodbca.so/\/usr\/lib\/x86_64-linux-gnu\/odbc\/psqlodbca.so/' /etc/odbcinst.ini
RUN sed --in-place 's/psqlodbcw.so/\/usr\/lib\/x86_64-linux-gnu\/odbc\/psqlodbcw.so/' /etc/odbcinst.ini

# Setup cargo and install cargo packages required for tests
USER vscode

# There is also a rust devcontainer, yet this way we get a toolchain
# which is updatable with rustup.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain stable -y

# We need 'parquet-read' to run the tests. At the moment I do not
# know a way to specify cargo installable executables as build
# dependencies.
RUN /home/vscode/.cargo/bin/cargo install parquet --features cli