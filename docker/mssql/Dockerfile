FROM mcr.microsoft.com/mssql/server:2019-CU5-ubuntu-18.04

COPY ./setup.sql .
COPY ./import-data.sh .
COPY ./entrypoint.sh .

# Normalize line endings if they've been modified locally (e.g. git checkout on Windows)
RUN apt-get update
RUN apt-get install -y dos2unix
RUN dos2unix ./import-data.sh
RUN dos2unix ./entrypoint.sh

# Switch back to mssql user and run the entrypoint script
USER mssql

ENV MSSQL_SA_PASSWORD="<YourStrong@Passw0rd>"

ENTRYPOINT ["/bin/bash", "./entrypoint.sh"]