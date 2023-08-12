use std::{
    fs::File,
    io::{ErrorKind, Write},
    path::Path,
    sync::Arc,
};

use assert_cmd::{assert::Assert, Command};
use lazy_static::lazy_static;
use odbc_api::{
    buffers::{BufferDesc, TextRowSet},
    sys::AttrConnectionPooling,
    Connection, ConnectionOptions, Cursor, Environment, IntoParameter,
};
use parquet::{
    column::writer::ColumnWriter,
    data_type::{ByteArray, FixedLenByteArray},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use predicates::{ord::eq, str::contains};
use tempfile::{tempdir, NamedTempFile};

const MSSQL: &str = "Driver={ODBC Driver 17 for SQL Server};\
    Server=localhost;\
    UID=SA;\
    PWD=My@Test@Password1;";

const POSTGRES: &str = "Driver={PostgreSQL UNICODE};\
    Server=localhost;\
    Port=5432;\
    Database=test;\
    Uid=test;\
    Pwd=test;";

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
lazy_static! {
    static ref ENV: Environment = {
        // Enable connection pools for faster test execution.
        //
        // # Safety
        //
        // This is safe because it is called once in the entire process. Therfore no race to this
        // setting can occurr. It is also called before intializing the environment, making it
        // certain that the newly created environment knows it is supposed to initialize the
        // connection pools.
        unsafe {
            Environment::set_connection_pooling(AttrConnectionPooling::OnePerDriver)
                .expect("ODBC manager must be able to initialize connection pools");
        }
        Environment::new().unwrap()
    };
}

// Use the parquet-read tool to verify values. It can be installed with `cargo install parquet`.
fn parquet_read_out(file: &str) -> Assert {
    let mut cmd = Command::new("parquet-read");
    cmd.args([file]).assert().success()
}

/// Assertions on the output of parquet_schema.
fn parquet_schema_out(file: &str) -> Assert {
    let mut cmd = Command::new("parquet-schema");
    cmd.args([file]).assert().success()
}

/// Query MSSQL database, yet do not specify username and password in the connection string, but
/// pass them as separate command line options.
#[test]
fn append_user_and_password_to_connection_string() {
    // Setup table for test
    let table_name = "AppendUserAndPasswordToConnectionString";
    TableMssql::new(table_name, &["VARCHAR(10)"]);

    // Connection string without user name and password.
    let connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=localhost;";
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name}");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--connection-string",
            connection_string,
            "--user",
            "SA",
            "--password",
            "My@Test@Password1",
            out_str,
            &query,
        ])
        .assert()
        .success();
}

#[test]
fn insert() {
    roundtrip("insert.par", "odbc2parquet_insert").success();
}

#[test]
fn insert_empty_document() {
    roundtrip("empty_document.par", "odbc2parquet_empty_document").success();
}

#[test]
fn nullable_parquet_buffers() {
    // Setup table for test
    let table_name = "NullableParquetBuffers";
    let mut table = TableMssql::new(table_name, &["VARCHAR(10)"]);
    table.insert_rows_as_text(&[[Some("Hello")], [None], [Some("World")], [None]]);

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name}");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    let expected = "\
        {a: \"Hello\"}\n\
        {a: null}\n\
        {a: \"World\"}\n\
        {a: null}\n\
    ";
    parquet_read_out(out_str).stdout(eq(expected));
}

#[test]
fn foobar_connection_string() {
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Tempfile path must be utf8");

    let mut cmd = Command::cargo_bin("odbc2parquet").unwrap();
    cmd.args([
        "-vvvv",
        "query",
        "-c",
        "foobar",
        out_str,
        "SELECT * FROM [uk-500$]",
    ])
    .assert()
    .failure()
    .code(1);
}

#[test]
fn should_give_good_error_if_specifying_directory_for_output() {
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // This SHOULD be a path to a file to create, but in this test, we specify a directory.
    let out_path = out_dir.path();
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Tempfile path must be utf8");

    let mut cmd = Command::cargo_bin("odbc2parquet").unwrap();
    cmd.args(["query", "-c", MSSQL, out_str, "SELECT 42 AS A"])
        .assert()
        .stderr(contains("Could not create output file '"))
        .failure()
        .code(1);
}

#[test]
fn parameters_in_query() {
    // Setup table for test
    let table_name = "ParamtersInQuery";
    let mut table = TableMssql::new(table_name, &["VARCHAR(10)", "INTEGER"]);
    table.insert_rows_as_text(&[["Wrong", "5"], ["Right", "42"]]);

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a,b FROM {table_name} WHERE b=?");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
            "42",
        ])
        .assert()
        .success();

    let expected = "\
        {a: \"Right\", b: 42}\n\
    ";
    parquet_read_out(out_str).stdout(eq(expected));
}

#[test]
fn should_allow_specifying_explicit_compression_level() {
    // Setup table for test
    let table_name = "ShouldAllowSpecifyingExplicitCompressionLevel";
    let mut table = TableMssql::new(table_name, &["VARCHAR(10)"]);
    table.insert_rows_as_text(&[["Hello"], ["World"]]);
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name}");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--connection-string",
            MSSQL,
            "--column-compression-default",
            "gzip",
            "--column-compression-level-default",
            "10",
            out_str,
            &query,
            "42",
        ])
        .assert()
        .success();
}

#[test]
fn query_sales() {
    // Setup table for test
    let table_name = "QuerySales";
    let mut table = TableMssql::new(table_name, &["DATE", "TIME(7)", "INT", "DECIMAL(10,2)"]);
    table.insert_rows_as_text(&[
        ["2020-09-09", "00:05:34", "54", "9.99"],
        ["2020-09-10", "12:05:32", "54", "9.99"],
        ["2020-09-10", "14:05:32", "34", "2.00"],
        ["2020-09-11", "06:05:12", "12", "-1.50"],
    ]);
    let expected_values = "\
        {a: 2020-09-09, b: 334000000000, c: 54, d: 9.99}\n\
        {a: 2020-09-10, b: 43532000000000, c: 54, d: 9.99}\n\
        {a: 2020-09-10, b: 50732000000000, c: 34, d: 2.00}\n\
        {a: 2020-09-11, b: 21912000000000, c: 12, d: -1.50}\n\
    ";
    let query = format!("SELECT a,b,c,d FROM {table_name} ORDER BY id");
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Tempfile path must be utf8");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    parquet_read_out(out_str).stdout(eq(expected_values));
}

#[test]
fn query_decimals() {
    // Setup table for test
    let table_name = "QueryDecimals";
    let mut table = TableMssql::new(
        table_name,
        &[
            "NUMERIC(3,2) NOT NULL",
            "DECIMAL(3,2) NOT NULL",
            "DECIMAL(3,0) NOT NULL",
            "DECIMAL(10,0) NOT NULL",
        ],
    );
    table.insert_rows_as_text(&[["1.23", "1.23", "3", "1234567890"]]);
    // let conn = ENV
    //     .connect_with_connection_string(MSSQL, ConnectionOptions::default())
    //     .unwrap();
    // setup_empty_table_mssql(
    //     &conn,
    //     table_name,
    //     &[
    //         "NUMERIC(3,2) NOT NULL",
    //         "DECIMAL(3,2) NOT NULL",
    //         "DECIMAL(3,0) NOT NULL",
    //         "DECIMAL(10,0) NOT NULL",
    //     ],
    // )
    // .unwrap();
    // let insert = format!("INSERT INTO {table_name} (a,b,c,d) VALUES (1.23, 1.23, 3, 1234567890);");
    // conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a,b,c,d FROM {table_name};");
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    let expected_values = "{a: 1.23, b: 1.23, c: 3., d: 1234567890.}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains(
        "message schema {\n  REQUIRED INT32 a (DECIMAL(3,2));\n  \
                REQUIRED INT32 b (DECIMAL(3,2));\n  \
                REQUIRED INT32 c (DECIMAL(3,0));\n  \
                REQUIRED INT64 d (DECIMAL(10,0));\n\
            }",
    ));
}

/// Produce output for downstream artefacts like polars which lack support for decimal. In effect
/// logical type decimal should not show up in the output
#[test]
fn query_decimals_avoid_decimal() {
    // Setup table for test
    let table_name = "QueryDecimalsAvoidDecimal";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(
        &conn,
        table_name,
        &[
            "NUMERIC(3,2) NOT NULL",
            "DECIMAL(3,2) NOT NULL",
            "DECIMAL(3,0) NOT NULL",
            "DECIMAL(10,0) NOT NULL",
        ],
    )
    .unwrap();
    let insert = format!("INSERT INTO {table_name} (a,b,c,d) VALUES (1.23, 1.23, 3, 1234567890);");
    conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a,b,c,d FROM {table_name};");
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            "--avoid-decimal",
            &query,
        ])
        .assert()
        .success();

    let expected_values = "{a: \"1.23\", b: \"1.23\", c: 3, d: 1234567890}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains(
        "message schema {\n  \
                REQUIRED BYTE_ARRAY a (UTF8);\n  \
                REQUIRED BYTE_ARRAY b (UTF8);\n  \
                REQUIRED INT32 c (INTEGER(32,true));\n  \
                REQUIRED INT64 d (INTEGER(64,true));\n\
            }",
    ));
}

/// Combination of avoid-decimal and int64-not-supported by driver. E.g. querying Decimal columns
/// from Oracle and using the output in polars
#[test]
fn query_decimals_avoid_decimal_int64_not_supported_by_driver() {
    // Setup table for test
    let table_name = "QueryDecimalsAvoidDecimalInt64NotSupportedByDriver";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DECIMAL(10,0) NOT NULL"]).unwrap();
    let insert = format!("INSERT INTO {table_name} (a) VALUES (1234567890);");
    conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name};");
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            "--avoid-decimal",
            "--driver-does-not-support-64bit-integers",
            &query,
        ])
        .assert()
        .success();

    let expected_values = "{a: 1234567890}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains(
        "message schema {\n  \
                REQUIRED INT64 a (INTEGER(64,true));\n\
            }",
    ));
}

#[test]
fn query_decimals_optional() {
    // Setup table for test
    let table_name = "QueryDecimalsOptional";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(
        &conn,
        table_name,
        &["NUMERIC(3,2)", "DECIMAL(3,2)", "DECIMAL(3,0)"],
    )
    .unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a,b,c)
        VALUES
        (1.23, 1.23, 123),
        (NULL, NULL, NULL),
        (4.56, 4.56, 456);"
    );
    conn.execute(&insert, ()).unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a,b,c FROM {table_name} ORDER BY id;");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    let expected_values =
        "{a: 1.23, b: 1.23, c: 123.}\n{a: null, b: null, c: null}\n{a: 4.56, b: 4.56, c: 456.}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));
}

/// Query a numeric/decimal which could be represented is i64 as text instead to accomondate missing
/// driver support for i64
#[test]
fn query_large_numeric_as_text() {
    // Setup table for test
    let table_name = "QueryLargeNumericAsText";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["NUMERIC(10,0) NOT NULL"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        (1234567890);"
    );
    conn.execute(&insert, ()).unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--driver-does-not-support-64bit-integers",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    parquet_read_out(out_str).stdout(eq("{a: 1234567890.}\n"));
    parquet_schema_out(out_str).stdout(contains("{\n  REQUIRED INT64 a (DECIMAL(10,0));\n}"));
}

#[test]
fn query_numeric_13_3() {
    // Setup table for test
    let table_name = "QueryNumeric13_3";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["NUMERIC(13,3) NOT NULL"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        (-1234567890.123);"
    );
    conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");
    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    let expected_values = "{a: -1234567890.123}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains("{\n  REQUIRED INT64 a (DECIMAL(13,3));\n}"));
}

#[test]
fn query_numeric_33_3() {
    // Setup table for test
    let table_name = "QueryNumeric33_3";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["NUMERIC(33,3) NOT NULL"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        (-123456789012345678901234567890.123);"
    );
    conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");
    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    let expected_values = "{a: -123456789012345678901234567890.123}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains(
        "{\n  REQUIRED FIXED_LEN_BYTE_ARRAY (14) a (DECIMAL(33,3));\n}",
    ));
}

#[test]
fn query_timestamp_with_timezone_mssql() {
    // Setup table for test
    let table_name = "QueryTimestampWithTimezone";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    // ODBC data type: SqlDataType(-155), column_size: 34, decimal_digits: 7
    setup_empty_table_mssql(&conn, table_name, &["DATETIMEOFFSET"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        ('2022-09-07 16:04:12.1234567 +02:00');"
    );
    conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");
    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    let expected_values = "{a: 1662559452123456700}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains("OPTIONAL INT64 a (TIMESTAMP(NANOS,true));"));
}

#[test]
fn query_timestamp_mssql_precision_7() {
    // Setup table for test
    let table_name = "QueryTimestampMssqlPrecision7";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DATETIME2(7)"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        ('2022-09-07 16:04:12.1234567');"
    );
    conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");
    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    let expected_values = "{a: 1662566652123456700}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains("OPTIONAL INT64 a (TIMESTAMP(NANOS,false));"));
}

#[test]
fn query_timestamp_ms_with_timezone_mssql() {
    // Setup table for test
    let table_name = "QueryTimestampMsWithTimezone";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    // ODBC data type: SqlDataType(-155), decimal_digits: 3
    setup_empty_table_mssql(&conn, table_name, &["DATETIMEOFFSET(3)"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        ('2022-09-07 16:04:12 +02:00');"
    );
    conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");
    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    let expected_values = "{a: 2022-09-07 14:04:12 +00:00}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains("OPTIONAL INT64 a (TIMESTAMP(MILLIS,true));"));
}

#[test]
fn query_time_mssql() {
    // Setup table for test
    let table_name = "QueryTime";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["TIME"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        ('16:04:12');"
    );
    conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");
    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    let expected_values = "{a: 57852000000000}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains("OPTIONAL INT64 a (TIME(NANOS,false));"));
}

#[test]
fn query_time_0_mssql() {
    // Setup table for test
    let table_name = "QueryTime0";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["TIME(0)"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        ('16:04:12');"
    );
    conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");
    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    // We can not use parquet read, as it uses the record API, which does not allow for the TIME type.
    // let expected_values = "{a: 57852000000000}\n";
    // parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains("OPTIONAL INT32 a (TIME(MILLIS,false));"));
}

#[test]
fn query_timestamp_with_timezone_postgres() {
    // Setup table for test
    let table_name = "QueryTimestampWithTimezone";
    let conn = ENV
        .connect_with_connection_string(POSTGRES, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_pg(&conn, table_name, &["TIMESTAMPTZ"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        ('2022-09-07 16:04:12 +02:00');"
    );
    conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");
    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            POSTGRES,
            &query,
        ])
        .assert()
        .success();

    let expected_values = "{a: 2022-09-07 14:04:12 +00:00}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains("OPTIONAL INT64 a (TIMESTAMP(MICROS,false));"));
}

#[test]
fn query_timestamp_postgres() {
    // Setup table for test
    let table_name = "QueryTimestamp";
    let conn = ENV
        .connect_with_connection_string(POSTGRES, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_pg(&conn, table_name, &["TIMESTAMP"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        ('2022-09-07 16:04:12');"
    );
    conn.execute(&insert, ()).unwrap();
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");
    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            POSTGRES,
            &query,
        ])
        .assert()
        .success();

    let expected_values = "{a: 2022-09-07 16:04:12 +00:00}\n";
    parquet_read_out(out_str).stdout(eq(expected_values));

    parquet_schema_out(out_str).stdout(contains("OPTIONAL INT64 a (TIMESTAMP(MICROS,false));"));
}

#[test]
fn query_all_the_types() {
    // Setup table for test
    let table_name = "AllTheTypes";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(
        &conn,
        table_name,
        &[
            "CHAR(5) NOT NULL",
            "NUMERIC(3,2) NOT NULL",
            "DECIMAL(3,2) NOT NULL",
            "INTEGER NOT NULL",
            "SMALLINT NOT NULL",
            "FLOAT(3) NOT NULL",
            "REAL NOT NULL",
            "DOUBLE PRECISION NOT NULL",
            "VARCHAR(100) NOT NULL",
            "DATE NOT NULL",
            "TIME NOT NULL",
            "DATETIME NOT NULL",
        ],
    )
    .unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a,b,c,d,e,f,g,h,i,j,k,l)
        VALUES
        ('abcde', 1.23, 1.23, 42, 42, 1.23, 1.23, 1.23, 'Hello, World!', '2020-09-16', '03:54:12', '2020-09-16 03:54:12');"
    );
    conn.execute(&insert, ()).unwrap();

    let expected_values = "{\
        a: \"abcde\", \
        b: 1.23, \
        c: 1.23, \
        d: 42, \
        e: 42, \
        f: 1.23, \
        g: 1.23, \
        h: 1.23, \
        i: \"Hello, World!\", \
        j: 2020-09-16, \
        k: 14052000000000, \
        l: 2020-09-16 03:54:12 +00:00\
    }\n";

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a,b,c,d,e,f,g,h,i,j,k,l FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    parquet_read_out(out_str).stdout(eq(expected_values));
}

#[test]
fn query_bits() {
    // Setup table for test
    let table_name = "QueryBits";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["BIT"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        (0), (1), (NULL), (1), (0);"
    );
    conn.execute(&insert, ()).unwrap();

    let expected_values = "\
        {a: false}\n\
        {a: true}\n\
        {a: null}\n\
        {a: true}\n\
        {a: false}\n\
    ";

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    parquet_read_out(out_str).stdout(eq(expected_values));
}

#[test]
fn query_doubles() {
    // Setup table for test
    let table_name = "QueryDoubles";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DOUBLE PRECISION NOT NULL"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        (0.1), (2.3);"
    );
    conn.execute(&insert, ()).unwrap();

    let expected_values = "\
        {a: 0.1}\n\
        {a: 2.3}\n\
    ";

    let expected_schema = "REQUIRED DOUBLE a;";

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    parquet_read_out(out_str).stdout(eq(expected_values));

    // Also verify schema to ensure f64 is choosen and not f32
    parquet_schema_out(out_str).stdout(contains(expected_schema));
}

/// Should not create a file if the query comes back empty `--no-empty-file` is set.
#[test]
fn query_comes_back_with_no_rows() {
    // Setup table for test
    let table_name = "QueryComesBackWithNoRows";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DOUBLE PRECISION NOT NULL"]).unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--connection-string",
            MSSQL,
            "--no-empty-file",
            out_str,
            &query,
        ])
        .assert()
        .success();

    assert_eq!(
        ErrorKind::NotFound,
        File::open(out_str).err().unwrap().kind()
    );
}

/// Should read query from standard input if "-" is provided as query text.
#[test]
fn read_query_from_stdin() {
    // Setup table for test
    let table_name = "ReadQueryFromStdin";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["INT"]).unwrap();
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        (54),
        (54),
        (34),
        (12);"
    );
    conn.execute(&insert, ()).unwrap();

    let expected_values = "\
        {a: 54}\n\
        {a: 54}\n\
        {a: 34}\n\
        {a: 12}\n\
    ";
    let query = format!("SELECT a FROM {table_name} ORDER BY id");
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Tempfile path must be utf8");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(["-vvvv", "query", out_str, "--connection-string", MSSQL, "-"])
        .write_stdin(query)
        .assert()
        .success();

    parquet_read_out(out_str).stdout(eq(expected_values));
}

#[test]
fn split_files_on_num_row_groups() {
    // Setup table for test
    let table_name = "SplitFilesOnNumRowGroups";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["INTEGER"]).unwrap();
    let insert = format!("INSERT INTO {table_name} (A) VALUES(1),(2),(3)");
    conn.execute(&insert, ()).unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name}");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            "--batch-size-row",
            "1",
            "--row-groups-per-file",
            "1",
            &query,
        ])
        .assert()
        .success();

    // Expect one file per row in table (3)

    parquet_read_out(out_dir.path().join("out_01.par").to_str().unwrap());
    parquet_read_out(out_dir.path().join("out_02.par").to_str().unwrap());
    parquet_read_out(out_dir.path().join("out_03.par").to_str().unwrap());
}

#[test]
fn split_files_on_size_limit() {
    // Setup table for test
    let table_name = "SplitFilesOnSizeLimit";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["INTEGER"]).unwrap();
    let insert = format!("INSERT INTO {table_name} (A) VALUES(1),(2),(3)");
    conn.execute(&insert, ()).unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name}");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            "--batch-size-row",
            "1",
            "--file-size-threshold",
            "1B",
            &query,
        ])
        .assert()
        .success();

    // Expect one file per row in table (3)

    parquet_read_out(out_dir.path().join("out_01.par").to_str().unwrap());
    parquet_read_out(out_dir.path().join("out_02.par").to_str().unwrap());
    parquet_read_out(out_dir.path().join("out_03.par").to_str().unwrap());
}

#[test]
fn configurable_suffix_length() {
    // Setup table for test
    let table_name = "ConfigurableSuffixLength";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["INTEGER"]).unwrap();
    let insert = format!("INSERT INTO {table_name} (A) VALUES(1)");
    conn.execute(&insert, ()).unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name}");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            "--batch-size-row",
            "1",
            "--file-size-threshold",
            "1B",
            "--suffix-length",
            "4",
            &query,
        ])
        .assert()
        .success();

    // Expect one file per row in table (3)

    parquet_read_out(out_dir.path().join("out_0001.par").to_str().unwrap());
}

#[test]
fn varbinary_column() {
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();

    setup_empty_table_mssql(&conn, "VarbinaryColumn", &["VARBINARY(10)"]).unwrap();
    conn.execute(
        "INSERT INTO VarbinaryColumn (a) Values \
        (CONVERT(Binary(5), 'Hello')),\
        (CONVERT(Binary(5), 'World')),\
        (NULL)",
        (),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = "SELECT a FROM VarbinaryColumn;";

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            query,
        ])
        .assert()
        .success();

    let expected = "{a: [72, 101, 108, 108, 111]}\n{a: [87, 111, 114, 108, 100]}\n{a: null}\n";

    parquet_read_out(out_str).stdout(eq(expected));
}

/// Since VARCHARMAX reports a size of 0, it will be ignored, resulting in an output file with no
/// columns. Yet odbc2parquet should detect this and give the user an error instead.
#[test]
fn query_varchar_max() {
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    let table_name = "QueryVarcharMax";

    setup_empty_table_mssql(&conn, table_name, &["VARCHAR(MAX)"]).unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) Values ('Hello'), ('World');"),
        (),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name};");

    // VARCHAR(max) has size 0. => Column is ignored and file would be empty and schemaless
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--connection-string",
            MSSQL,
            out_str,
            &query,
        ])
        .assert()
        .failure();
}

/// Since VARCHARMAX reports a size of 0, it will be ignored, resulting in an output file with no
/// columns. Yet by setting a size limit we can make it work.
#[test]
fn query_varchar_max_with_column_length_limit() {
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    let table_name = "QueryVarcharMaxWithColumnLengthLimit";

    setup_empty_table_mssql(&conn, table_name, &["VARCHAR(MAX)"]).unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) Values ('Hello'), ('World');"),
        (),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name};");

    // VARCHAR(max) has size 0. => Column is ignored and file would be empty and schemaless
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--connection-string",
            MSSQL,
            "--column-length-limit",
            "4096",
            out_str,
            &query,
        ])
        .assert()
        .success();
}

/// Introduced after discovering a bug, that columns were not ignored on windows.
///
/// Since VARCHARMAX reports a size of 0, it will be ignored, resulting in an output file with no
/// columns. Yet odbc2parquet should detect this and give the user an error instead.
#[test]
fn query_varchar_max_utf16() {
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    let table_name = "QueryVarcharMaxUtf16";

    setup_empty_table_mssql(&conn, table_name, &["VARCHAR(MAX)"]).unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) Values ('Hello'), ('World');"),
        (),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name};");

    // VARCHAR(max) has size 0. => Column is ignored and file would be empty and schemaless
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--encoding",
            "Utf16",
            "--connection-string",
            MSSQL,
            out_str,
            &query,
        ])
        .assert()
        .failure();
}

#[test]
fn binary_column() {
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();

    setup_empty_table_mssql(&conn, "BinaryColumn", &["BINARY(5)"]).unwrap();
    conn.execute(
        "INSERT INTO BinaryColumn (a) Values \
        (CONVERT(Binary(5), 'Hello')),\
        (CONVERT(Binary(5), 'World')),\
        (NULL)",
        (),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = "SELECT a FROM BinaryColumn;";

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            query,
        ])
        .assert()
        .success();

    let expected = "{a: [72, 101, 108, 108, 111]}\n{a: [87, 111, 114, 108, 100]}\n{a: null}\n";
    parquet_read_out(out_str).stdout(eq(expected));
}

/// The prefer-varbinary flag must enforce mapping of binary colmuns to BYTE_ARRAY instead of
/// FIXED_LEN_BYTE_ARRAY.
#[test]
fn prefer_varbinary() {
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();

    let table_name = "PreferVarbinary";

    setup_empty_table_mssql(&conn, table_name, &["BINARY(5)"]).unwrap();
    conn.execute(
        &format!(
            "INSERT INTO {table_name} (a) Values \
        (CONVERT(Binary(5), 'Hello')),\
        (CONVERT(Binary(5), 'World')),\
        (NULL)"
        ),
        (),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--prefer-varbinary",
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    let expected = "{a: [72, 101, 108, 108, 111]}\n{a: [87, 111, 114, 108, 100]}\n{a: null}\n";
    parquet_read_out(out_str).stdout(eq(expected));

    parquet_schema_out(out_str).stdout(contains("OPTIONAL BYTE_ARRAY a;"));
}

/// Strings with interior nuls should be written into parquet file as they are.
#[test]
fn interior_nul_in_varchar() {
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, "InteriorNul", &["VARCHAR(10)"]).unwrap();

    conn.execute(
        "INSERT INTO InteriorNul (a) VALUES (?);",
        &"a\0b".into_parameter(),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = "SELECT a FROM InteriorNul;";

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            query,
        ])
        .assert()
        .success();

    let expected = "{a: \"a\0b\"}\n";

    parquet_read_out(out_str).stdout(eq(expected));
}

/// Fixed size NCHAR column on database is not truncated then value is passed into a narrow buffer.
#[test]
#[cfg(not(target_os = "windows"))] // Windows does not use UTF-8 as default system encoding
fn nchar_not_truncated() {
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    let table_name = "NCharNotTruncated";
    setup_empty_table_mssql(&conn, table_name, &["NCHAR(1)"]).unwrap();

    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?);", table_name),
        &"Ü".into_parameter(),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = &format!("SELECT a FROM {};", table_name);

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
            "-vvvv",
            "query",
            "--encoding",
            "system",
            "--connection-string",
            MSSQL,
            out_str,
            query,
        ])
        .assert()
        .success();

    let expected = "{a: \"Ü\"}\n";

    parquet_read_out(out_str).stdout(eq(expected));
}

/// Test non ASCII character with system encoding
#[test]
#[cfg(not(target_os = "windows"))] // Windows does not use UTF-8 as default system encoding
fn system_encoding() {
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    let table_name = "SystemEncoding";
    setup_empty_table_mssql(&conn, table_name, &["VARCHAR(10)"]).unwrap();

    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?);", table_name),
        &"Ü".into_parameter(),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = &format!("SELECT a FROM {};", table_name);

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
            "-vvvv",
            "query",
            "--encoding",
            "system",
            "--connection-string",
            MSSQL,
            out_str,
            query,
        ])
        .assert()
        .success();

    let expected = "{a: \"Ü\"}\n";

    parquet_read_out(out_str).stdout(eq(expected));
}

/// Test non ASCII character with utf16 encoding
#[test]
fn utf_16_encoding() {
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    let table_name = "Utf16Encoding";
    setup_empty_table_mssql(&conn, table_name, &["VARCHAR(10)"]).unwrap();

    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (?);"),
        &"Ü".into_parameter(),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = &format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--encoding",
            "utf16",
            "--connection-string",
            MSSQL,
            out_str,
            query,
        ])
        .assert()
        .success();

    let expected = "{a: \"Ü\"}\n";

    parquet_read_out(out_str).stdout(eq(expected));
}

/// Test non ASCII character with automatic codec detection
#[test]
fn auto_encoding() {
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    let table_name = "AutoEncoding";
    setup_empty_table_mssql(&conn, table_name, &["VARCHAR(1)"]).unwrap();

    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (?);"),
        &"Ü".into_parameter(),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = &format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--encoding",
            "auto",
            "--connection-string",
            MSSQL,
            out_str,
            query,
        ])
        .assert()
        .success();

    let expected = "{a: \"Ü\"}\n";

    parquet_read_out(out_str).stdout(eq(expected));
}

#[test]
pub fn insert_32_bit_integer() {
    let table_name = "Insert32BitInteger";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["INTEGER"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED INT32 a;
        }
    ";

    write_values_to_file(message_type, &input_path, &[42i32, 5, 1], None);

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("42\n5\n1", actual);
}

#[test]
pub fn insert_optional_32_bit_integer() {
    let table_name = "InsertOptional32BitInteger";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["INTEGER"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL INT32 a;
        }
    ";

    write_values_to_file(message_type, &input_path, &[42i32, 1], Some(&[1, 0, 1]));

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("42\nNULL\n1", actual);
}

#[test]
pub fn insert_64_bit_integer() {
    let table_name = "Insert64BitInteger";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["INTEGER"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED INT64 a;
        }
    ";

    write_values_to_file(message_type, &input_path, &[-42i64, 5, 1], None);

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("-42\n5\n1", actual);
}

#[test]
pub fn insert_optional_64_bit_integer() {
    let table_name = "InsertOptional64BitInteger";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["BIGINT"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL INT64 a;
        }
    ";

    write_values_to_file(message_type, &input_path, &[-42i64, 1], Some(&[1, 0, 1]));

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("-42\nNULL\n1", actual);
}

#[test]
pub fn insert_utf8() {
    let table_name = "InsertUtf8";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["VARCHAR(50)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED BYTE_ARRAY a (UTF8);
        }
    ";

    let text: ByteArray = "Hello, World!".into();
    write_values_to_file(
        message_type,
        &input_path,
        &[text, "Hallo, Welt!".into(), "Bonjour, Monde!".into()],
        None,
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("Hello, World!\nHallo, Welt!\nBonjour, Monde!", actual);
}

#[test]
pub fn insert_optional_utf8() {
    let table_name = "InsertOptionalUtf8";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["VARCHAR(50)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL BYTE_ARRAY a (UTF8);
        }
    ";

    let text: ByteArray = "Hello, World!".into();
    write_values_to_file(
        message_type,
        &input_path,
        &[text, "Hallo, Welt!".into()],
        Some(&[1, 0, 1]),
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("Hello, World!\nNULL\nHallo, Welt!", actual);
}

#[test]
pub fn insert_utf16() {
    let table_name = "InsertUtf16";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["VARCHAR(50)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED BYTE_ARRAY a (UTF8);
        }
    ";

    let text: ByteArray = "Hello, World!".into();
    write_values_to_file(
        message_type,
        &input_path,
        &[text, "Hallo, Welt!".into(), "Bonjour, Monde!".into()],
        None,
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            "--encoding",
            "Utf16",
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("Hello, World!\nHallo, Welt!\nBonjour, Monde!", actual);
}

#[test]
pub fn insert_optional_utf16() {
    let table_name = "InsertOptionalUtf16";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["VARCHAR(50)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL BYTE_ARRAY a (UTF8);
        }
    ";

    let text: ByteArray = "Hello, World!".into();
    write_values_to_file(
        message_type,
        &input_path,
        &[text, "Hallo, Welt!".into()],
        Some(&[1, 0, 1]),
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            "--encoding",
            "Utf16",
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("Hello, World!\nNULL\nHallo, Welt!", actual);
}

#[test]
pub fn insert_bool() {
    let table_name = "InsertBool";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["BIT"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED BOOLEAN a;
        }
    ";

    write_values_to_file(message_type, &input_path, &[true, false, false], None);

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1\n0\n0", actual);
}

#[test]
pub fn insert_optional_bool() {
    let table_name = "InsertOptionalBool";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["BIT"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL BOOLEAN a;
        }
    ";

    write_values_to_file(message_type, &input_path, &[true, false], Some(&[1, 0, 1]));

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1\nNULL\n0", actual);
}

#[test]
pub fn insert_f32() {
    let table_name = "InsertF32";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["FLOAT"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED FLOAT a;
        }
    ";

    write_values_to_file(message_type, &input_path, &[1.2f32, 3.4, 5.6], None);

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(
        "1.2000000476837158\n3.4000000953674316\n5.5999999046325684",
        actual
    );
}

#[test]
pub fn insert_optional_f32() {
    let table_name = "InsertOptionalF32";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["FLOAT"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL FLOAT a;
        }
    ";

    write_values_to_file(message_type, &input_path, &[1.2f32, 3.4], Some(&[1, 0, 1]));

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1.2000000476837158\nNULL\n3.4000000953674316", actual);
}

#[test]
pub fn insert_f64() {
    let table_name = "InsertF64";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["FLOAT(53)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED DOUBLE a;
        }
    ";

    write_values_to_file(message_type, &input_path, &[1.2f64, 3.4, 5.6], None);

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1.2\n3.3999999999999999\n5.5999999999999996", actual);
}

#[test]
pub fn insert_optional_f64() {
    let table_name = "InsertOptionalF64";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["FLOAT(53)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL DOUBLE a;
        }
    ";

    write_values_to_file(message_type, &input_path, &[1.2f64, 3.4], Some(&[1, 0, 1]));

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1.2\nNULL\n3.3999999999999999", actual);
}

#[test]
pub fn insert_date() {
    let table_name = "InsertDate";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DATE"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED INT32 a (DATE);
        }
    ";

    write_values_to_file(message_type, &input_path, &[0i32, 365, 18695], None);

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1970-01-01\n1971-01-01\n2021-03-09", actual);
}

#[test]
pub fn insert_optional_date() {
    let table_name = "InsertOptionalDate";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["Date"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL INT32 a (DATE);
        }
    ";

    write_values_to_file(message_type, &input_path, &[0i32, 18695], Some(&[1, 0, 1]));

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1970-01-01\nNULL\n2021-03-09", actual);
}

#[test]
pub fn insert_time_ms() {
    let table_name = "InsertTimeMs";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["TIME(3)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED INT32 a (TIME_MILLIS);
        }
    ";

    // Total number of milli seconds since midnight
    write_values_to_file(
        message_type,
        &input_path,
        &[0i32, 3_600_000, 82_800_000],
        None,
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("00:00:00.000\n01:00:00.000\n23:00:00.000", actual);
}

#[test]
pub fn insert_optional_time_ms() {
    let table_name = "InsertOptionalTimeMs";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["TIME(3)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL INT32 a (TIME_MILLIS);
        }
    ";

    // Total number of milli seconds since midnight
    write_values_to_file(
        message_type,
        &input_path,
        &[0i32, 82_800_000],
        Some(&[1, 0, 1]),
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("00:00:00.000\nNULL\n23:00:00.000", actual);
}

#[test]
pub fn insert_time_us() {
    let table_name = "InsertTimeUs";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["TIME(6)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED INT64 a (TIME_MICROS);
        }
    ";

    // Total number of milli seconds since midnight
    write_values_to_file(
        message_type,
        &input_path,
        &[0i64, 3_600_000_000, 82_800_000_000],
        None,
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("00:00:00.000000\n01:00:00.000000\n23:00:00.000000", actual);
}

#[test]
pub fn insert_optional_time_us() {
    let table_name = "InsertOptionalTimeUs";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["TIME(6)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL INT64 a (TIME_MICROS);
        }
    ";

    // Total number of milli seconds since midnight
    write_values_to_file(
        message_type,
        &input_path,
        &[0i64, 82_800_000_000],
        Some(&[1, 0, 1]),
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("00:00:00.000000\nNULL\n23:00:00.000000", actual);
}

#[test]
pub fn insert_decimal_from_i32() {
    let table_name = "InsertDecimalFromI32";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED INT32 a (DECIMAL(9,2));
        }
    ";

    // Total number of milli seconds since midnight
    write_values_to_file(message_type, &input_path, &[0i32, 123456789, -42], None);

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\n1234567.89\n-.42", actual);
}

#[test]
pub fn insert_decimal_from_i32_optional() {
    let table_name = "InsertDecimalFromI32Optional";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL INT32 a (DECIMAL(9,2));
        }
    ";

    // Total number of milli seconds since midnight
    write_values_to_file(message_type, &input_path, &[0i32, -42], Some(&[1, 0, 1]));

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\nNULL\n-.42", actual);
}

#[test]
pub fn insert_decimal_from_i64() {
    let table_name = "InsertDecimalFromI64";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED INT64 a (DECIMAL(9,2));
        }
    ";

    // Total number of milli seconds since midnight
    write_values_to_file(message_type, &input_path, &[0i64, 123456789, -42], None);

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\n1234567.89\n-.42", actual);
}

#[test]
pub fn insert_decimal_from_i64_optional() {
    let table_name = "InsertDecimalFromI64Optional";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL INT64 a (DECIMAL(9,2));
        }
    ";

    // Total number of milli seconds since midnight
    write_values_to_file(message_type, &input_path, &[0i64, -42], Some(&[1, 0, 1]));

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\nNULL\n-.42", actual);
}

#[test]
pub fn insert_timestamp_ms() {
    let table_name = "InsertTimestampMs";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DATETIME2"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED INT64 a (TIMESTAMP_MILLIS);
        }
    ";

    // Total number of milli seconds since unix epoch
    write_values_to_file(message_type, &input_path, &[0i64, 1, 1616367053000], None);

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(
        "1970-01-01 00:00:00.0000000\n1970-01-01 00:00:00.0010000\n2021-03-21 22:50:53.0000000",
        actual
    );
}

#[test]
pub fn insert_timestamp_ms_optional() {
    let table_name = "InsertTimestampMsOptional";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DATETIME2"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL INT64 a (TIMESTAMP_MILLIS);
        }
    ";

    // Total number of milli seconds since unix epoch
    write_values_to_file(
        message_type,
        &input_path,
        &[0i64, 1616367053000],
        Some(&[1, 0, 1]),
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(
        "1970-01-01 00:00:00.0000000\nNULL\n2021-03-21 22:50:53.0000000",
        actual
    );
}

#[test]
pub fn insert_timestamp_us() {
    let table_name = "InsertTimestampUs";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DATETIME2"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED INT64 a (TIMESTAMP_MICROS);
        }
    ";

    // Total number of milli seconds since unix epoch
    write_values_to_file(
        message_type,
        &input_path,
        &[0i64, 1, 1616367053000000],
        None,
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(
        "1970-01-01 00:00:00.0000000\n1970-01-01 00:00:00.0000010\n2021-03-21 22:50:53.0000000",
        actual
    );
}

#[test]
pub fn insert_timestamp_us_optional() {
    let table_name = "InsertTimestampUsOptional";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DATETIME2"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL INT64 a (TIMESTAMP_MICROS);
        }
    ";

    // Total number of milli seconds since unix epoch
    write_values_to_file(
        message_type,
        &input_path,
        &[0i64, 1616367053000000],
        Some(&[1, 0, 1]),
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(
        "1970-01-01 00:00:00.0000000\nNULL\n2021-03-21 22:50:53.0000000",
        actual
    );
}

#[test]
pub fn insert_binary() {
    let table_name = "InsertBinary";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["VARBINARY(50)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED BYTE_ARRAY a;
        }
    ";

    let text: ByteArray = "Hello, World!".into();
    write_values_to_file(
        message_type,
        &input_path,
        &[text, "Hallo, Welt!".into(), "Bonjour, Monde!".into()],
        None,
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(
        "48656C6C6F2C20576F726C6421\n48616C6C6F2C2057656C7421\n426F6E6A6F75722C204D6F6E646521",
        actual
    );
}

#[test]
pub fn insert_binary_optional() {
    let table_name = "InsertBinaryOptional";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["VARBINARY(50)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL BYTE_ARRAY a;
        }
    ";

    let text: ByteArray = "Hello, World!".into();
    write_values_to_file(
        message_type,
        &input_path,
        &[text, "Hallo, Welt!".into()],
        Some(&[1, 0, 1]),
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(
        "48656C6C6F2C20576F726C6421\nNULL\n48616C6C6F2C2057656C7421",
        actual
    );
}

#[test]
pub fn insert_fixed_len_binary() {
    let table_name = "InsertFixedLenBinary";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["BINARY(13)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED FIXED_LEN_BYTE_ARRAY(13) a;
        }
    ";

    let to_fixed_len_byte_array = |input: &str| -> FixedLenByteArray {
        let ba: ByteArray = input.into();
        ba.into()
    };

    let text: FixedLenByteArray = to_fixed_len_byte_array("Hello, World!");
    write_values_to_file(
        message_type,
        &input_path,
        &[
            text,
            to_fixed_len_byte_array("Hallo, Welt!!"),
            to_fixed_len_byte_array("0123456789012"),
        ],
        None,
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(
        "48656C6C6F2C20576F726C6421\n48616C6C6F2C2057656C742121\n30313233343536373839303132",
        actual
    );
}

#[test]
pub fn insert_fixed_len_binary_optional() {
    let table_name = "InsertFixedLenBinaryOptional";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["BINARY(13)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL FIXED_LEN_BYTE_ARRAY(13) a;
        }
    ";

    let to_fixed_len_byte_array = |input: &str| -> FixedLenByteArray {
        let ba: ByteArray = input.into();
        ba.into()
    };

    let text: FixedLenByteArray = to_fixed_len_byte_array("Hello, World!");
    write_values_to_file(
        message_type,
        &input_path,
        &[text, to_fixed_len_byte_array("0123456789012")],
        Some(&[1, 0, 1]),
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(
        "48656C6C6F2C20576F726C6421\nNULL\n30313233343536373839303132",
        actual
    );
}

#[test]
pub fn insert_decimal_from_binary() {
    let table_name = "InsertDecimalFromBinary";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED BYTE_ARRAY a (DECIMAL(9,2));
        }
    ";

    let zero: ByteArray = vec![0u8].into();
    write_values_to_file(
        message_type,
        &input_path,
        &[
            zero,
            vec![1].into(),
            vec![1, 0, 0].into(),
            vec![255, 255, 255].into(),
            vec![255].into(),
        ],
        None,
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\n.01\n655.36\n-.01\n-.01", actual);
}

#[test]
pub fn insert_decimal_from_binary_optional() {
    let table_name = "InsertDecimalFromBinaryOptional";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL BYTE_ARRAY a (DECIMAL(9,2));
        }
    ";

    let zero: ByteArray = vec![0u8].into();
    write_values_to_file(
        message_type,
        &input_path,
        &[zero, vec![1].into()],
        Some(&[1, 0, 1]),
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\nNULL\n.01", actual);
}

#[test]
pub fn insert_decimal_from_fixed_binary() {
    let table_name = "InsertDecimalFromFixedBinary";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DECIMAL(5,2)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            REQUIRED FIXED_LEN_BYTE_ARRAY(3) a (DECIMAL(5,2));
        }
    ";

    let to_fixed_len_byte_array = |input: Vec<u8>| -> FixedLenByteArray {
        let ba: ByteArray = input.into();
        ba.into()
    };

    let zero: FixedLenByteArray = to_fixed_len_byte_array(vec![0u8, 0, 0]);
    write_values_to_file(
        message_type,
        &input_path,
        &[
            zero,
            to_fixed_len_byte_array(vec![0, 0, 1]),
            to_fixed_len_byte_array(vec![1, 0, 0]),
            to_fixed_len_byte_array(vec![255, 255, 255]),
        ],
        None,
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\n.01\n655.36\n-.01", actual);
}

#[test]
pub fn insert_decimal_from_fixed_binary_optional() {
    let table_name = "InsertDecimalFromFixedBinaryOptional";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["DECIMAL(5,2)"]).unwrap();

    // Prepare file

    // A temporary directory, to be removed at the end of the test.
    let tmp_dir = tempdir().unwrap();
    // The name of the input parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let input_path = tmp_dir.path().join("input.par");

    let message_type = "
        message schema {
            OPTIONAL FIXED_LEN_BYTE_ARRAY(3) a (DECIMAL(5,2));
        }
    ";

    let to_fixed_len_byte_array = |input: Vec<u8>| -> FixedLenByteArray {
        let ba: ByteArray = input.into();
        ba.into()
    };

    write_values_to_file(
        message_type,
        &input_path,
        &[
            to_fixed_len_byte_array(vec![0, 0, 1]),
            to_fixed_len_byte_array(vec![255, 255, 255]),
        ],
        Some(&[1, 0, 1]),
    );

    // Insert file into table
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            input_path.to_str().unwrap(),
            table_name,
        ])
        .assert()
        .success();

    // Query table and check for expected result
    let query = format!("SELECT a FROM {table_name} ORDER BY Id");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".01\nNULL\n-.01", actual);
}

/// Write query output to stdout
#[test]
pub fn write_query_result_to_stdout() {
    // Given
    let table_name = "WriteQueryResultToStdout";
    // Prepare table
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["INTEGER"]).unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (?)"),
        [42i32, 5, 64].as_slice(),
    )
    .unwrap();

    // When

    // Query table and write contents to stdout
    let query = format!("SELECT a FROM {table_name} ORDER BY id");
    let command = Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--connection-string",
            MSSQL,
            "-", // Use `-` to explicitly write to stdout
            &query,
        ])
        .assert()
        .success();

    // Then
    let output = &command.get_output().stdout;
    assert!(!output.is_empty());

    // Write captured contents of stdout to temporary file so we can inspect it with read parquet
    let mut output_file = NamedTempFile::new().unwrap();
    output_file.write_all(output).unwrap();

    let expected = "{a: 42}\n";

    let output_path = output_file.path().to_str().unwrap();
    parquet_read_out(output_path).stdout(eq(expected));
}

/// Write query output to stdout
#[test]
pub fn reject_writing_to_stdout_and_file_size_limit() {
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--connection-string",
            "FakeConnectionString",
            "--file-size-threshold",
            "1GiB",
            "-", // Use `-` to explicitly write to stdout
            "SELECT a FROM FakeTableName ORDER BY id",
        ])
        .assert()
        .failure()
        .stderr(contains(
            "file-size-threshold conflicts with specifying stdout ('-') as output.",
        ));
}

/// This did not work in earlier versions there we set the batch write size of the parquet writer to
/// the ODBC batch size.
#[test]
#[ignore = "Takes too long to run"]
fn query_4097_bits() {
    let num_bits = 4097;

    // Setup table for test
    let table_name = "Query4097Bits";
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    setup_empty_table_mssql(&conn, table_name, &["BIT"]).unwrap();

    // Insert 4097 bits "false" (default constructed) into the table
    let insert = format!(
        "INSERT INTO {table_name}
        (a)
        VALUES
        (?);"
    );
    let desc = BufferDesc::Bit { nullable: false };
    let mut parameter_buffer = conn
        .prepare(&insert)
        .unwrap()
        .into_column_inserter(num_bits, [desc])
        .unwrap();
    parameter_buffer.set_num_rows(num_bits);
    parameter_buffer.execute().unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {table_name};");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();
}

/// Writes a parquet file with one row group and one column.
fn write_values_to_file<T>(
    message_type: &str,
    input_path: &Path,
    values: &[T],
    def_levels: Option<&[i16]>,
) where
    T: WriteToCw,
{
    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(WriterProperties::builder().build());
    let file = File::create(input_path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();
    let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
    T::write_batch(col_writer.untyped(), values, def_levels);
    col_writer.close().unwrap();
    row_group_writer.close().unwrap();
    writer.close().unwrap();
}

const COLUMN_NAMES: &[&str] = &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"];

/// Sets up a table in the mssql database and allows us to fill it with data. Column names are given
/// automatically a,b,c, etc.
pub struct TableMssql<'a, const NUM_COLUMNS: usize> {
    pub name: &'a str,
    pub conn: Connection<'a>,
}

impl<'a, const NUM_COLUMNS: usize> TableMssql<'a, NUM_COLUMNS> {
    pub fn new(name: &'a str, column_types: &'a [&'a str; NUM_COLUMNS]) -> Self {
        let conn = ENV
            .connect_with_connection_string(
                MSSQL,
                ConnectionOptions {
                    login_timeout_sec: Some(5),
                },
            )
            .expect("Must be able to connect to MSSQL database.");
        setup_empty_table_mssql(&conn, name, column_types)
            .expect("Must be able to setup empty table.");
        TableMssql { name, conn }
    }

    pub fn insert_rows_as_text<'b, C>(&mut self, content: &[[C; NUM_COLUMNS]])
    where
        C: Into<Option<&'b str>> + Copy,
    {
        let statement = self.insert_statement(NUM_COLUMNS);
        // Insert everything in one go => capacity == length of array
        let capacity = content.len();
        let max_str_len = (0..NUM_COLUMNS)
            .map(|col_index| {
                (0..content.len())
                    .map(|row_index| {
                        let opt: Option<&str> = content[row_index][col_index].into();
                        opt.map(|s| s.len()).unwrap_or(0)
                    })
                    .max()
                    .unwrap_or(0)
            })
            .collect::<Vec<_>>();
        let mut inserter = self
            .conn
            .prepare(&statement)
            .unwrap()
            .into_text_inserter(capacity, max_str_len)
            .unwrap();

        for (row_index, row) in content.iter().enumerate() {
            for (column_index, element) in row.iter().enumerate() {
                let element: Option<&str> = (*element).into();
                let element = element.map(|s| s.as_bytes());
                inserter
                    .column_mut(column_index)
                    .set_cell(row_index, element);
            }
        }

        inserter.set_num_rows(content.len());
        inserter.execute().unwrap();
    }

    fn insert_statement(&self, number_of_columns: usize) -> String {
        // A string like e.g. "a,b,c"
        let columns = COLUMN_NAMES[..number_of_columns].join(",");
        let placeholders = vec!["?"; number_of_columns].join(",");
        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.name, columns, placeholders
        )
    }
}

/// Creates the table and assures it is empty. Columns are named a,b,c, etc.
pub fn setup_empty_table_mssql(
    conn: &Connection,
    table_name: &str,
    column_types: &[&str],
) -> Result<(), odbc_api::Error> {
    let identity = "int IDENTITY(1,1)";
    setup_empty_table(table_name, column_types, conn, identity)
}

fn setup_empty_table(
    table_name: &str,
    column_types: &[&str],
    conn: &Connection,
    identity: &str,
) -> Result<(), odbc_api::Error> {
    let drop_table = &format!("DROP TABLE IF EXISTS {table_name}");
    let cols = column_types
        .iter()
        .zip(COLUMN_NAMES)
        .map(|(ty, name)| format!("{name} {ty}"))
        .collect::<Vec<_>>()
        .join(", ");
    let create_table = format!("CREATE TABLE {table_name} (id {identity},{cols});");
    conn.execute(drop_table, ())?;
    conn.execute(&create_table, ())?;
    Ok(())
}

/// Creates the table and assures it is empty (adapted for PostgreSQL). Columns are named a,b,c, etc.
pub fn setup_empty_table_pg(
    conn: &Connection,
    table_name: &str,
    column_types: &[&str],
) -> Result<(), odbc_api::Error> {
    let identity = "SERIAL PRIMARY KEY";
    setup_empty_table(table_name, column_types, conn, identity)
}

/// Test helper using two commands to roundtrip parquet to and from a data source.
///
/// # Parameters
///
/// * `file`: File used in the roundtrip. Table schema is currently hardcoded.
/// * `table_name`: Each test must use its unique table name, to avoid race conditions with other
///   tests.
fn roundtrip(file: &'static str, table_name: &str) -> Assert {
    // Setup table for test. We use the table name only in this test.
    let conn = ENV
        .connect_with_connection_string(MSSQL, ConnectionOptions::default())
        .unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {table_name}"), ())
        .unwrap();
    conn.execute(
        &format!("CREATE TABLE {table_name} (country VARCHAR(255), population BIGINT);"),
        (),
    )
    .unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let in_path = format!("tests/{file}");

    // Insert parquet
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            &in_path,
            table_name,
        ])
        .assert()
        .success();

    // Query as parquet
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args([
            "-vvvv",
            "query",
            "--connection-string",
            MSSQL,
            out_str,
            &format!("SELECT country, population FROM {table_name} ORDER BY population;"),
        ])
        .assert();

    let expectation = String::from_utf8(
        std::process::Command::new("parquet-read")
            .args(&[&in_path][..])
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();

    parquet_read_out(out_str).stdout(expectation)
}

/// Consumes a cursor and generates a CSV string from the result set.
fn cursor_to_string(mut cursor: impl Cursor) -> String {
    let batch_size = 20;
    let mut buffer = TextRowSet::for_cursor(batch_size, &mut cursor, None).unwrap();
    let mut row_set_cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let mut text = String::new();

    while let Some(row_set) = row_set_cursor.fetch().unwrap() {
        for row_index in 0..row_set.num_rows() {
            if row_index != 0 {
                text.push('\n');
            }
            for col_index in 0..row_set.num_cols() {
                if col_index != 0 {
                    text.push(',');
                }
                text.push_str(
                    row_set
                        .at_as_str(col_index, row_index)
                        .unwrap()
                        .unwrap_or("NULL"),
                );
            }
        }
    }

    text
}

trait WriteToCw: Sized {
    fn write_batch(col_writer: &mut ColumnWriter, values: &[Self], def_levels: Option<&[i16]>);
}

macro_rules! impl_write_to_cw {
    ($type:ty , $variant:ident) => {
        impl WriteToCw for $type {
            fn write_batch(
                col_writer: &mut ColumnWriter,
                values: &[Self],
                def_levels: Option<&[i16]>,
            ) {
                match col_writer {
                    ColumnWriter::$variant(ref mut cw) => {
                        cw.write_batch(values, def_levels, None).unwrap();
                    }
                    _ => panic!("Unexpected Column Writer type"),
                }
            }
        }
    };
}

impl_write_to_cw!(bool, BoolColumnWriter);
impl_write_to_cw!(i32, Int32ColumnWriter);
impl_write_to_cw!(i64, Int64ColumnWriter);
impl_write_to_cw!(f32, FloatColumnWriter);
impl_write_to_cw!(f64, DoubleColumnWriter);
impl_write_to_cw!(ByteArray, ByteArrayColumnWriter);
impl_write_to_cw!(FixedLenByteArray, FixedLenByteArrayColumnWriter);
