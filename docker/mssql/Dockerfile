FROM mcr.microsoft.com/mssql/server:2019-CU5-ubuntu-18.04

# Switch back to mssql user and run the entrypoint script
USER mssql

ENV MSSQL_SA_PASSWORD="<YourStrong@Passw0rd>"

ENTRYPOINT ["/opt/mssql/bin/sqlservr", "--accept-eula", "--reset-sa-password"]