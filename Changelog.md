# Changelog

## 0.6.22

* Introduce flag `--driver-does-not-support-64bit-integers` in order to compensate for missing 64 Bit integer support in the Oracle driver.

## 0.6.21

* Updated dependencies
  * Including update to `parquet 8.0.0`

## 0.6.20

* Updated dependencies
  * Including update to `parquet 7.0.0`

## 0.6.19

* New `Completion` subcommand to generate shell completions.
* Fix: An issue with not reserving enough memories for the largest possible string if the octet length reported by the driver is to small. Now calculation is based on column sized.

## 0.6.18

* Update dependencies.
* Includes upstream fix: Passwords containing a `+` character are now escaped if passed via the
  `--password` command line option.

## 0.6.17

* Update dependencies.

## 0.6.16

* Update dependencies.

## 0.6.15

* Use less memory for Text columns.

## 0.6.14

* Update dependencies

## 0.6.13

* Fix: Version number

## 0.6.12

* Fix: An issue with the mapping of ODBC data type FLOAT has been resolved. Before it had always
  been mapped to 32 Bit floating point precision. Now the precision of that column is also taken
  into account to map it to a 64 Bit floating point in case the precision exceeds 24.

## 0.6.11

* Optimization: Required columns which do not require conversion to parquet types during fetch, are
  now no longer copied in the intermediate buffer. This will result in a little bit less memory
  usage and faster processing required (i.e. NOT NULL) columns with types:

  * Double
  * Real
  * Float
  * TinyInteger
  * SmallInteger
  * Integer
  * Big Int
  * and Decimals with Scale 0 and precision <= 18.

## 0.6.10

* Fix: An issue with the ODBC buffer allocated for `NUMERIC` and `DECIMAL` types being two bytes to
  short, which could lead to wrong values being written into parquet without emmitting an error
  message.

## 0.6.9

* Both `--batch-size-row` and `--batch-size-mib` can now be both specified together.
* If no `--batch-size-*` limit is specified. A row limit of 65535 is now also applied by default, next to the size limit.

## 0.6.8

* Fixed an issue where large batch sizes could cause failures writing Boolean columns.
* Updated dependencies

## 0.6.7

* Updated dependencies

## 0.6.6

* Updated dependencies
* Allow specifyig fallback encodings for output parquet files then using the `query` subcommand.

## 0.6.5

* Updated dependencies.
* Better error message in case unixODBC version is outdated.

## 0.6.4

* Default log level is now warning. `--quiet` flag has been introduced to supress warnings, if desired.
* Introduce the `--prefer-varbinary` flag to the `query` subcommand. It allows for mapping `BINARY` SQL colmuns to `BYTE_ARRAY` instead of `FIXED_LEN_BYTE_ARRAY`. This flag has been introduced in an effort to increase the compatibility of the output with spark.

## 0.6.3

* Fix: Columns for which the driver reported size zero were not ignored then UTF-16 encoding had been enabled. This is the default setting for Windows. These columns are now complete missing from the output file, instead of the column being present with all values being NULL or empty strings.

## 0.6.2

* Introduced support for connecting via GUI on windows platforms via `--prompt` flag.

## 0.6.1

* Introduced `--column-compression-default` in order to allow users to specify column compression.
* Default column compression is now `gzip`.

## 0.6.0

* Requires at least Rust 1.51.0 to build.
* Command line parameters `user` and `password` will no longer be ignored then passed together with a connection string. Instead their values will be appended as `UID` and `PWD` attributes at the end.
* `--batch-size` command line flag has been renamed to `batch-size-row`.
* Introduced `--batch-size-mib` command line flag to limit batch size based on memory usage
* Default batch size is adapted so buffer allocation requires 2 GiB on 64 Bit platforms and 1 GiB on 32 Bit Platforms.
* Fix: There is now an error message produced if the resulting parquet file would not contain any columns.

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
