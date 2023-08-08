# ODBC to Parquet

[![Licence](https://img.shields.io/crates/l/odbc2parquet)](https://github.com/pacman82/odbc2parquet/blob/master/License)
[![Crates.io](https://img.shields.io/crates/v/odbc2parquet)](https://crates.io/crates/odbc2parquet)

A command line tool to query an ODBC data source and write the result into a parquet file.

* Small memory footprint. Only holds one batch at a time in memory.
* Fast. Makes efficient use of ODBC bulk reads, to lower IO overhead.
* Flexible. Query any ODBC data source you have a driver for. MySQL, MS SQL, Excel, ...

## Mapping of types in queries

The tool queries the ODBC Data source for type information and maps it to parquet type as such:

| ODBC SQL Type              | Parquet Type                 |
|----------------------------|------------------------------|
| Decimal(p < 39, s)         | Decimal(p,s)                 |
| Numeric(p < 39, s)         | Decimal(p,s)                 |
| Bit                        | Boolean                      |
| Double                     | Double                       |
| Real                       | Float                        |
| Float(p: 0..24)            | Float                        |
| Float(p >= 25)             | Double                       |
| Tiny Integer               | Int8                         |
| Small Integer              | Int16                        |
| Integer                    | Int32                        |
| Big Int                    | Int64                        |
| Date                       | Date                         |
| Time(p: 0..3)*             | Time Milliseconds            |
| Time(p: 4..6)*             | Time Microseconds            |
| Time(p: 7..9)*             | Time Nanoseconds             |
| Timestamp(p: 0..3)         | Timestamp Milliseconds       |
| Timestamp(p: 4..6)         | Timestamp Microseconds       |
| Timestamp(p >= 7)          | Timestamp Nanoseconds        |
| Datetimeoffset(p: 0..3)    | Timestamp Milliseconds (UTC) |
| Datetimeoffset(p >= 4)     | Timestamp Microseconds (UTC) |
| Varbinary                  | Byte Array                   |
| Long Varbinary             | Byte Array                   |
| Binary                     | Fixed Length Byte Array      |
| All others                 | Utf8 Byte Array              |

`p` is short for `precision`. `s` is short for `scale`. Intervals are inclusive.
* Time is only supported for Microsoft SQL Server

## Installation

### Prerequisites

To work with this tool you need an ODBC driver manager and an ODBC driver for the data source you want to access.

#### Windows

An ODBC driver manager is already preinstalled on windows. So is the `ODBC data sources (64Bit)` and `ODBC data sources (32Bit)` app which you can use to discover which drivers are already available on your system.

#### Linux

This tool links both at runtime and during build against `libodbc.so`. To get it you should install [unixODBC](http://www.unixodbc.org/). You can do this using your systems packet manager. For *ubuntu* you run:

```shell
sudo apt install unixodbc-dev
```

#### OS-X

This tool links both at runtime and during build against `libodbc.so`. To get it you should install [unixODBC](http://www.unixodbc.org/). To install it I recommend the [homebrew](https://brew.sh/) packet manager, which allows you to install it using:

```shell
brew install unixodbc
```

### Download binary from GitHub

<https://github.com/pacman82/odbc2parquet/releases/latest>

*Note*: Download the 32 Bit version if you want to connect to data sources using 32 Bit drivers and download the 64 Bit version if you want to connect via 64 Bit drivers. It won't work vice versa.

### Via Cargo

If you have a rust tool chain installed, you can install this tool via cargo.

```shell script
cargo install odbc2parquet
```

You can install `cargo` from here <https://rustup.rs/>.

## Usage

Use `odbc2parquet --help` to see all commands.

### Query

Use `odbc2parquet help query` to see all options related to fetching data.

#### Query using connection string

```bash
odbc2parquet query \
--connection-string "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;" \
out.par  \
"SELECT * FROM Birthdays"
```

#### Query using data source name

```bash
odbc2parquet query \
--dsn my_db \
--password "<YourStrong@Passw0rd>" \
--user "SA" \
out.par1 \
"SELECT * FROM Birthdays"
```

#### Use parameters in query

```shell
odbc2parquet query \
--connection-string "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;" \
out.par  \
"SELECT * FROM Birthdays WHERE year > ? and year < ?" \
1990 2010
```

### List available ODBC drivers

```bash
odbc2parquet list-drivers
```

### List available ODBC data sources

```bash
odbc2parquet list-data-sources
```

### Inserting data into a database

```shell
odbc2parquet insert \
--connection-string "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;" \
input.par \
MyTable
```

Use `odbc2parquet help insert` to see all options related to inserting data.

## Links

Thanks to @samaguire there is a script for Powershell users which helps you to download a bunch of tables to a folder: <https://github.com/samaguire/odbc2parquet-PSscripts>
