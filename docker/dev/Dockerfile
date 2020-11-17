# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.148.0/containers/rust/.devcontainer/base.Dockerfile

FROM mcr.microsoft.com/vscode/devcontainers/rust:0-1

# Install Microsoft ODBC SQL Drivers (msodbcsql17 package) for Debian 10
# https://docs.microsoft.com/de-de/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN echo msodbcsql17 msodbcsql/ACCEPT_EULA boolean true | debconf-set-selections

# [Optional] Uncomment this section to install additional packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends unixodbc-dev msodbcsql17
