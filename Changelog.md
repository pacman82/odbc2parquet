# Changelog

## 6.0.7

* Binary release for Ubuntu ARM architectures. Thanks @sindelevich

## 6.0.2-6

* Fix release 6.0.1

## 6.0.1

* Binary release names for artifacts now end in architecture rather native bit size

## 6.0.0

* File extensions are now retained then splitting files. E.g. if `--output` is 'my_results.parquet' and split into two files they will be named 'my_results_01.parquet' and 'my_results_02.parquet'. Previously there has been always the ending '.par' attached.

## 5.1.2

* Fix: 5.1.0 introduced a regression, which caused output file enumeration to happen even if file splitting is not activated, if `--no-empty-file` had been set.

## 5.1.1

* Fix: 5.1.0 introduced a regression, which caused output file enumeration to be start with a suffix of `2` instead of `1` if in addition to file splitting the `--no-empty-file` flag had also been set.

## 5.1.0

* Additional log message with info level emitting the number of total number of rows written, the file size and the path for each file.

## 5.0.0

* Removed flag `--driver_returns_memory_garbage_for_indicators`. Turns out the issue with IBM DB/2 drivers which triggered this can better be solved using a version of their ODBC driver which ends in `o` and is compiled with a 64Bit size for `SQLLEN`.
* Release for MacOS M1 (Thanks to free tier of fly.io)

## 4.1.3

* Updated dependencies, including a bug fix in decimal parsing. Negative decimals smaller than 1 would have misjuged as positive.

## 4.1.2

* Updated dependencies, including an update to `parquet-rs 50`

## 4.1.1

* Decimal parsing is now more robust, against different radix characters and missing trailing zeroes.

## 4.1.0

* Introduced flag `--driver-returns-memory-garbage-for-indicators`. This is a reaction to witnessing IBM DB2/Linux drivers filling the indicator arrays with memory garbage. Activating this flag will activate a workaround using terminating zeroes to determine string length. `odbc2parquet` will not be able to distinguish between empty strings and NULL anymore with this active and map everything to NULL. Currently the workaround is only active for UTF-8 encoded payloads.

## 4.0.0

* Default compression is now `zstd` with level `3`.

## 3.1.2

* Fix: If ODBC drivers report `-4` (`NO_TOTAL`) as display size, the size can now be controlled with `--column-length-limit`. The issue occurred for JSON columns with MySQL.

## 3.1.1

* Fix: Invalid UTF-16 encoding emitted from the data source will now cause an error instead of a panic.

## 3.1.0

* Additional log message emitting the number of total rows fetched so far.

## 3.0.1

* Fix: `--no-empty-file` now works correctly with options causing files to be splitted like `--file-size-threshold` or `--row-groups-per-file`.

## 3.0.0

* Zero sized columns are now treated as an error. Before `odbc2parquet` issued a warning and ignored them.
* Fix: `--file-size-threshold` had an issue with not resetting the current file size after starting a new file. This caused only the first file to have the desired size. All subsequent files would contain only one row group each.

## 2.0.4

* Fix: `--column-length-limit` not only caused large variadic columns to have a sensible upper bound, but also caused columns with known smaller bound to allocate just as much memory, effectivly wasting a lot of memory in some scenarios. In this version the limit will only be applied if the column length actually exceeds the length limit specified.
* Fix: Some typos in the `--help` text have been fixed.

## 2.0.3

* Fix: Then fetching relational type `TINYINT`, the driver is queried for the signess of the column. The result is now reflected in the logical type written into parquet. In the past the `TINYINT` has always been assumed to be signed, even if the ODBC driver would have described the column as unsigned.

## 2.0.2

* Fix: The `--help` subcommand for query wrongly listed `bit-packed` as supported.

## 2.0.1

* Explicitly check for lower and upper bound if writing timestamps with nanoseconds precision into a parquet file. Timestamps have to be between `1677-09-21 00:12:44` and `2262-04-11 23:47:16.854775807`. Emit an error and abort if bound checks fails.

## 2.0.0

* Write timestamps with precision greater than seven as nanoseconds

## 1.0.0

* Write Parquet Version 2.0
* Establish semantic versioning
* Update dependencies

## 0.15.6

* Update dependencies

## 0.15.5

* Improves error message in case creating an output file fails.

## 0.15.4

* Accidential release from branch.

## 0.15.3

* Adds option `--column-compression-level-default` to specify compression level explicitly.

## 0.15.2

* Introduced now option for `query` subcommand `--column-length-limit` to limit the memory allocated for an indivual variadic column of the result set.

## 0.15.1

* Updated dependencies

## 0.15.0

* Time(p: 7..) is mapped to Timestamp Nanoseconds for Microsoft SQL Server
* Time(p: 4..=6) is mapped to Timestamp Microseconds for Microsoft SQL Server
* Time(p: 0..=3) is mapped to Timestamp Milliseconds for Microsoft SQL Server
* Introduced new flag for `query` subcommand `--no-empty-file` which prevents creation of an output file in case the query comes back with `0` rows.

## 0.14.1

* Updated dependencies

## 0.14.0

* Time(p) is mapped to Timestamp Nanoseconds for Microsoft SQL Server

## 0.13.3

* Fix: Fixed an issue there setting the `--column-compression-default` to `snappy` did result in the column compression default actually being set to `zstd`.

## 0.13.2

* Introduce flag `avoid-decmial` to produce output without logical type `DECIMAL`. This allows artefacts without decimal support to process the output of `odbc2parquet`.

## 0.13.1

* The level of verbosity had been one to high:
  * `--quiet` now suppresses warning messages as intended.
  * `-v` -> `Info`
  * `-vv` -> `Debug`

## 0.13.0

* `DATETIMEOFFSET` on `Microsoft SQL Server` now is mapped to `TIMESTAMP` with instant semantics i.e. its mapped to UTC and understood to reference a specific point in time, rather than a wall clock time.

## 0.12.1

* Allow specifying ODBC connection string via environment variable `ODBC_CONNECTION_STRING` instead of `--connection_string` option.

## 0.12.0

* Pad suffixes `_01` with leading zeroes to make the file names more friendly for lexical sorting if splitting fetch output. Number is padded to two digits by default.
* Updated dependencies

## 0.11.0

* Use narrow text on non-windows platforms by default. Connection strings, queries and error messages are assumed to be UTF-8 and not transcoded to and from UTF-16.

## 0.10.0

* Physical type of `DECIMAL` is now `INT64` instead of `FIXED_LEN_BYTE_ARRAY` if precision does not exceed 18.
* Physical type of `DECIMAL` is now `INT32` instead of `FIXED_LEN_BYTE_ARRAY` if precision does not exceed 9.
* Dropped support for Decimals and a Numeric with precision higher than `38`. Please open issue if required. Microsoft SQL does support this type up to this precision so currently there is no easy way to test for `DECIMAL`s which can not be represented as `i128`.
* Fetching decimal columns with scale `0` and `--driver-does-not-support-64bit-integers` now specifies the logical type as `DECIMAL`. Physical type remains a 64 Bit Integer.
* Updated dependencies

## 0.9.5

* Updated dependencies

## 0.9.4

* Pass `-` as query string to read the query statement text from standard in instead.

## 0.9.2-3

* Updated dependencies
* Release binary artifact for `x86_64-ubuntu`.

## 0.9.1

* Introduced flag `--no-color` which allows to supress emitting colors for the log output.

## 0.9.0

* `query` now allows for specifying `-` as a positional output argument in order to stream to standard out instead of writing to a file.

## 0.8.0

* `--batches-per-file` is now named `--row-groups-per-file`.
* New `query` option `--file-size-threshold`.

## 0.7.1

* Fixed bug causing `--batch-size-memory` to be interpreted as many times the specified limit.

## 0.7.0

* Updated dependencies. Including `parquet 15.0.0`
* `query` option `--batch-size-mib` is now `--batch-size-memory` and allows specifying inputs with SI units. E.g. `2GiB`.

## 0.6.32

* Updated dependencies. Improvements in upstream `odbc-api` may lead to faster insertion if using many batches.

## 0.6.31

* Updated dependencies. Including `parquet 14.0.0`

## 0.6.30

* Updated dependencies.
* Undo: Recover from failed memory allocations of binary and text buffers, because of unclear performance implications.

## 0.6.29

* Recover from failed memory allocations of binary and text buffers and terminate the tool gracefully.

## 0.6.28

* Update dependencies. This includes an upstream improvement in `odbc-api 0.36.1` which emits a better error if the `unixODBC` version does not support `ODBC 3.80`.

## 0.6.27

* Updated dependencies

## 0.6.26

* Updated dependencies

## 0.6.25

Peace for the citizens of Ukraine who fight for their freedom and stand up to oppression. Peace for the Russian soldier, who does not know why he is shooting at his brothers and sisters, may he be reunited with his family soon.

Peace to 🇺🇦, 🇷🇺 and the world. May sanity prevail.

## 0.6.24

* Updating dependencies.
* Added message for Oracle users telling them about the `--driver-does-not-support-64bit-integers`, if SQLFetch fails with `HY004`.

## 0.6.23

* Update dependencies. Including `parquet 9.0.2`.

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
