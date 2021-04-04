# Changelog

## 0.5.11 (next version)

* Requires at least Rust 1.51.0 to build.
* Command line parameters `user` and `password` will no longer be ignored then passed together with a connection string. Instead their values will be appended as `UID` and `PWD` attributes at the end.

## 0.5.10

* Add new sub command `insert`.

## 0.5.9

* Fix: Right truncation of values in fixed sized `NCHAR` columns had occurred if a character in the value did use more than one byte in UTF-8 encoding (or more than two bytes for UTF-16).

## 0.5.8

* Fix: On windows platforms the tool is now using UTF-16 encoding by default to exchange character data with the data source. The behaviour has been changed since on most windows platform the system locale is not configured to use UTF-8. The behaviour can be configured manually on any platform using the newly introduced `--encoding` option.

## 0.5.7

* Fix: Interior nuls within `VARCHAR` values did cause the tool to panic. Now these values are written into parquet as is.

## 0.5.6

* Fix: Replace non UTF-8 characters with the UTF-8 replacement character (`�`). ODBC encodes string according to the current locale, so this issue could cause non UTF-8 characters to be written into Parquet Text columns on Windows systems. If a non UTF-8 character is encountered a warning is generated hinting at the user to change to a UTF-8 locale.

## 0.5.5

* `VARBINARY` and `BINARY` SQL columns are now mapped unto `BYTE_ARRAY` and `FIXED_LEN_BYTE_ARRAY` parquet physical types.

## 0.5.4

* Update dependencies
* Builds with stable Rust

## 0.5.3

* Update to `parquet 3.0.0`.

## 0.5.2

* Maps ODBC `Timestamp`s with precision <= 3 to parquet `TIMESTAMP_MILLISECONDS`.
* Updated dependencies

## 0.5.1

* Introduces option `--batches-per-file` in order to define an upper limit for batches in a single output file and split output across multiple files.

## 0.5.0

* Fix: Microsoft SQL Server user defined types with unbounded lengths have been mapped to Text columns with length zero. This caused at least one warning per row. These columns are now ignored at the beginning, causing exactly one warning. They also do no longer appear in the output schema.
* Fix: Allocate extra space in text column for multi byte UTF-8 characters.
* SQL Numeric and Decimal are now always mapped to the parquet Decimal independent of the precision or scale. The 32Bit or 62Bit "physical" Integer representation is chosen for SQL types with Scale Null and a precision smaller ten or 19, otherwise the "physical" type is to be a fixed size byte array.

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
