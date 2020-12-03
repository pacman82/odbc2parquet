# Changelog

## 0.4.2

* Fix: Tool could panic if too many warnings were generated at once.

## 0.4.1

* Introduces subcommand `list-data-sources`.

## 0.4.0

* Introduces subcommands. `query` is now required to `query` the database and store contents into parquet.
* Introduces `drivers` subcommand.

## 0.3.0

* Adds support for parameterized queries.

## 0.2.1

* Fix: A major issue caused columns containing NULL values to either cause a panic or even worse, produce a parquet file with wrong data in the affected column without showing any error at all.

## 0.2.0

* Binary release of 32 Bit Window executable on GitHub
* Binary release for OS-X
* Connection string is no longer a positional argument.
* Allow connecting to an ODBC datasource using dsn.

## 0.1.6

* Binary release of 64 Bit Window executable on GitHub

## 0.1.5

* Maps ODBC `Bit` to Parquet `Boolean`.
* Maps ODBC `Tinyint` to Parquet `INT 8`.
* Maps ODBC `Real` to Parquet `Float`.
* Maps ODBC `Numeric` same as it would `DECIMAL`.

## 0.1.4

* Default row group size is now `100000`.
* Adds support for Decimal types with precision 0..18 and scale = 0 (i.e. Everything that has a straightforward `i32` or `i64` representation).

## 0.1.3

* Fix: Fixed an issue there some column types were not bound to the cursor, which let to some column only containing `NULL` or `0`.

## 0.1.2

* Retrieve column names more reliably with a greater range of drivers.

## 0.1.1

* Log batch number and numbers of rows at info level.
* Log bound and detected ODBC type.
* Auto generate names for unnamed columns.

## 0.1.0

Initial release
