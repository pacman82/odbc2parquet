# ODBC to Parquet

A command line tool to query an ODBC data source and write the result into a parquet file.

## Usage

### Query using connection string

```shell
odbc2parquet --connection-string "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;" "SELECT * FROM Birthdays" out.par1
```

### Query using data source name

```shell
odbc2parquet --dsn my_db --password "<YourStrong@Passw0rd>" --user "SA" "SELECT * FROM Birthdays" out.par1
```

Use `odbc2parquet --help` to see all option.

## Installation

Currently only deployed via cargo. Use `cargo +nightly install odbc2parquet` to install it.

## Mapping of types

The tool queries the ODBC Data source for type information and maps it to parquet type as such:

| ODBC SQL Type         | Parquet Logical Type   |
|-----------------------|------------------------|
| Decimal(p=0..18, s=0) | Decimal(p,s)           |
| Numeric(p=0..18, s=0) | Decimal(p,s)           |
| Bit                   | Boolean                |
| Double                | Double                 |
| Real                  | Float                  |
| Float                 | Float                  |
| Tiny Integer          | Int8                   |
| Small Integer         | Int16                  |
| Integer               | Int32                  |
| Big Int               | Int64                  |
| Date                  | Date                   |
| Timestamp             | Timestamp Microseconds |
| All others            | Utf8 Byte Array        |

`p` is short for `precision`. `s` is short for `scale`. Intervals are inculsive the last element.
