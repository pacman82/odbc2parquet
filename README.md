# ODBC to Parquet

Query ODBC databases and save the result in a parquet file.

## Usage

```shell
odbc2parquet run "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;" "SELECT * FROM Birthdays" out.par1
```

Use `odbc2parquet --help` to see all option.

## Installation

Currently only deployed via cargo. Use `cargo +nightly install odbc2parquet` to install it.

## Mapping of types

The tool queries the ODBC Data source for type information and maps it to parquet type as such:

| ODBC SQL Type | Parquet Logical Type   |
|---------------|------------------------|
| Double        | Double                 |
| Float         | Float                  |
| Small Integer | Int16                  |
| Integer       | Int32                  |
| Big Int       | Int64                  |
| Date          | Date                   |
| Timestamp     | Timestamp Microseconds |
| All others    | Utf8 Byte Array        |
