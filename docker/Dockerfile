FROM mcr.microsoft.com/mssql/server:2019-CU5-ubuntu-18.04

COPY ./setup.sql .
COPY ./import-data.sh .
COPY ./entrypoint.sh .

# Switch back to mssql user and run the entrypoint script
USER mssql

ENV MSSQL_SA_PASSWORD="<YourStrong@Passw0rd>"

ENTRYPOINT ["/bin/bash", "./entrypoint.sh"]