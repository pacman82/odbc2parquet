use std::{fs::File, path::Path, sync::Arc};

use assert_cmd::{assert::Assert, Command};
use lazy_static::lazy_static;
use odbc_api::{buffers::TextRowSet, Connection, Cursor, Environment, IntoParameter};
use parquet::{
    column::writer::ColumnWriter,
    data_type::{ByteArray, FixedLenByteArray},
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::parser::parse_message_type,
};
use predicates::{ord::eq, str::contains};
use tempfile::tempdir;

const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
lazy_static! {
    static ref ENV: Environment = unsafe { Environment::new().unwrap() };
}

/// Query MSSQL database, yet do not specify username and password in the connection string, but
/// pass them as separate command line options.
#[test]
fn append_user_and_password_to_connection_string() {
    // Setup table for test
    let table_name = "AppendUserAndPasswordToConnectionString";
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(10)"]).unwrap();

    // Connection string without user name and password.
    let connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=localhost;";
    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {}", table_name);

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
            "-vvvv",
            "query",
            "--connection-string",
            connection_string,
            "--user",
            "SA",
            "--password",
            "<YourStrong@Passw0rd>",
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(10)"]).unwrap();
    let insert = format!(
        "INSERT INTO {} (A) VALUES('Hello'),(NULL),('World'),(NULL)",
        table_name
    );
    conn.execute(&insert, ()).unwrap();

    let expected = "\
        {a: \"Hello\"}\n\
        {a: null}\n\
        {a: \"World\"}\n\
        {a: null}\n\
    ";

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {}", table_name);

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(eq(expected));
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
    cmd.args(&[
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
fn parameters_in_query() {
    // Setup table for test
    let table_name = "ParamtersInQuery";
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(10)", "INTEGER"]).unwrap();
    let insert = format!(
        "INSERT INTO {} (A,B) VALUES('Wrong', 5),('Right', 42)",
        table_name
    );
    conn.execute(&insert, ()).unwrap();

    let expected = "\
        {a: \"Right\", b: 42}\n\
    ";

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a,b FROM {} WHERE b=?", table_name);

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
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

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(eq(expected));
}

#[test]
fn query_sales() {
    // Setup table for test
    let table_name = "QuerySales";
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(
        &conn,
        table_name,
        &["DATE", "TIME(0)", "INT", "DECIMAL(10,2)"],
    )
    .unwrap();
    let insert = format!(
        "INSERT INTO {}
        (a,b,c,d)
        VALUES
        ('2020-09-09', '00:05:34', 54, 9.99),
        ('2020-09-10', '12:05:32', 54, 9.99),
        ('2020-09-10', '14:05:32', 34, 2.00),
        ('2020-09-11', '06:05:12', 12, -1.50);",
        table_name
    );
    conn.execute(&insert, ()).unwrap();

    let expected_values = "\
        {a: 2020-09-09 +00:00, b: \"00:05:34\", c: 54, d: 9.99}\n\
        {a: 2020-09-10 +00:00, b: \"12:05:32\", c: 54, d: 9.99}\n\
        {a: 2020-09-10 +00:00, b: \"14:05:32\", c: 34, d: 2.00}\n\
        {a: 2020-09-11 +00:00, b: \"06:05:12\", c: 12, d: -1.50}\n\
    ";

    let query = format!("SELECT a,b,c,d FROM {} ORDER BY id", table_name);

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Tempfile path must be utf8");

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str)
        .assert()
        .success()
        .stdout(eq(expected_values));
}

#[test]
fn query_all_the_types() {
    // Setup table for test
    let table_name = "AllTheTypes";
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(
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
        "INSERT INTO {}
        (a,b,c,d,e,f,g,h,i,j,k,l)
        VALUES
        ('abcde', 1.23, 1.23, 42, 42, 1.23, 1.23, 1.23, 'Hello, World!', '2020-09-16', '03:54:12', '2020-09-16 03:54:12');",
        table_name
    );
    conn.execute(&insert, ()).unwrap();

    let expected_values = "{\
        a: \"abcde\", \
        b: 0.12, \
        c: 0.12, \
        d: 42, \
        e: 42, \
        f: 1.23, \
        g: 1.23, \
        h: 1.23, \
        i: \"Hello, World!\", \
        j: 2020-09-16 +00:00, \
        k: \"03:54:12.0000000\", \
        l: 2020-09-16 03:54:12 +00:00\
    }\n";

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a,b,c,d,e,f,g,h,i,j,k,l FROM {};", table_name);

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            &query,
        ])
        .assert()
        .success();

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str)
        .assert()
        .success()
        .stdout(eq(expected_values));
}

#[test]
fn split_files() {
    // Setup table for test
    let table_name = "SplitFiles";
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["INTEGER"]).unwrap();
    let insert = format!("INSERT INTO {} (A) VALUES(1),(2),(3)", table_name);
    conn.execute(&insert, ()).unwrap();

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Temporary file path must be utf8");

    let query = format!("SELECT a FROM {}", table_name);

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
            "-vvvv",
            "query",
            out_str,
            "--connection-string",
            MSSQL,
            "--batch-size-row",
            "1",
            "--batches-per-file",
            "1",
            &query,
        ])
        .assert()
        .success();

    // Expect one file per row in table (3)

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_dir.path().join("out_1.par").to_str().unwrap())
        .assert()
        .success();

    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_dir.path().join("out_2.par").to_str().unwrap())
        .assert()
        .success();

    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_dir.path().join("out_3.par").to_str().unwrap())
        .assert()
        .success();
}

#[test]
fn varbinary_column() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();

    setup_empty_table(&conn, "VarbinaryColumn", &["VARBINARY(10)"]).unwrap();
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
        .args(&[
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

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(eq(expected));
}

#[test]
fn query_varchar_max() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let table_name = "QueryVarcharMax";

    setup_empty_table(&conn, table_name, &["VARCHAR(MAX)"]).unwrap();
    conn.execute(
        &format!(
            "INSERT INTO {} (a) Values ('Hello'), ('World');",
            table_name
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

    let query = format!("SELECT a FROM {};", table_name);

    // VARCHAR(max) has size 0. => Column is ignored and file would be empty and schemaless
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
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

/// Introduced after discovering a bug, that columns were not ignored on windows.
#[test]
fn query_varchar_max_utf16() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let table_name = "QueryVarcharMaxUtf16";

    setup_empty_table(&conn, table_name, &["VARCHAR(MAX)"]).unwrap();
    conn.execute(
        &format!(
            "INSERT INTO {} (a) Values ('Hello'), ('World');",
            table_name
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

    let query = format!("SELECT a FROM {};", table_name);

    // VARCHAR(max) has size 0. => Column is ignored and file would be empty and schemaless
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();

    setup_empty_table(&conn, "BinaryColumn", &["BINARY(5)"]).unwrap();
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
        .args(&[
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

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(eq(expected));
}

/// The prefer-varbinary flag must enforce mapping of binary colmuns to BYTE_ARRAY instead of
/// FIXED_LEN_BYTE_ARRAY.
#[test]
fn prefer_varbinary() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();

    let table_name = "PreferVarbinary";

    setup_empty_table(&conn, table_name, &["BINARY(5)"]).unwrap();
    conn.execute(
        &format!(
            "INSERT INTO {} (a) Values \
        (CONVERT(Binary(5), 'Hello')),\
        (CONVERT(Binary(5), 'World')),\
        (NULL)",
            table_name
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

    let query = format!("SELECT a FROM {};", table_name);

    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
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

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(eq(expected));

    let mut cmd = Command::new("parquet-schema");
    cmd.arg(out_str)
        .assert()
        .success()
        .stdout(contains("OPTIONAL BYTE_ARRAY a;"));
}

/// Strings with interior nuls should be written into parquet file as they are.
#[test]
fn interior_nul_in_varchar() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, "InteriorNul", &["VARCHAR(10)"]).unwrap();

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
        .args(&[
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

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(eq(expected));
}

/// Fixed size NCHAR column on database is not truncated then value is passed into a narrow buffer.
#[test]
#[cfg(not(target_os = "windows"))] // Windows does not use UTF-8 as default system encoding
fn nchar_not_truncated() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let table_name = "NCharNotTruncated";
    setup_empty_table(&conn, table_name, &["NCHAR(1)"]).unwrap();

    conn.execute(&format!("INSERT INTO {} (a) VALUES ('Ü');", table_name), ())
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

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(eq(expected));
}

/// Test non ASCII character with system encoding
#[test]
#[cfg(not(target_os = "windows"))] // Windows does not use UTF-8 as default system encoding
fn system_encoding() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let table_name = "SystemEncoding";
    setup_empty_table(&conn, table_name, &["VARCHAR(10)"]).unwrap();

    conn.execute(&format!("INSERT INTO {} (a) VALUES ('Ü');", table_name), ())
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

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(eq(expected));
}

/// Test non ASCII character with utf16 encoding
#[test]
fn utf_16_encoding() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let table_name = "Utf16Encoding";
    setup_empty_table(&conn, table_name, &["VARCHAR(10)"]).unwrap();

    conn.execute(&format!("INSERT INTO {} (a) VALUES ('Ü');", table_name), ())
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
            "utf16",
            "--connection-string",
            MSSQL,
            out_str,
            query,
        ])
        .assert()
        .success();

    let expected = "{a: \"Ü\"}\n";

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(eq(expected));
}

/// Test non ASCII character with automatic codec detection
#[test]
fn auto_encoding() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let table_name = "AutoEncoding";
    setup_empty_table(&conn, table_name, &["VARCHAR(10)"]).unwrap();

    conn.execute(&format!("INSERT INTO {} (a) VALUES ('Ü');", table_name), ())
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
            "auto",
            "--connection-string",
            MSSQL,
            out_str,
            query,
        ])
        .assert()
        .success();

    let expected = "{a: \"Ü\"}\n";

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(eq(expected));
}

#[test]
pub fn insert_32_bit_integer() {
    let table_name = "Insert32BitInteger";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["INTEGER"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("42\n5\n1", actual);
}

#[test]
pub fn insert_optional_32_bit_integer() {
    let table_name = "InsertOptional32BitInteger";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["INTEGER"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("42\nNULL\n1", actual);
}

#[test]
pub fn insert_64_bit_integer() {
    let table_name = "Insert64BitInteger";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["INTEGER"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("-42\n5\n1", actual);
}

#[test]
pub fn insert_optional_64_bit_integer() {
    let table_name = "InsertOptional64BitInteger";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BIGINT"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("-42\nNULL\n1", actual);
}

#[test]
pub fn insert_utf8() {
    let table_name = "InsertUtf8";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(50)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("Hello, World!\nHallo, Welt!\nBonjour, Monde!", actual);
}

#[test]
pub fn insert_optional_utf8() {
    let table_name = "InsertOptionalUtf8";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(50)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("Hello, World!\nNULL\nHallo, Welt!", actual);
}

#[test]
pub fn insert_utf16() {
    let table_name = "InsertUtf16";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(50)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("Hello, World!\nHallo, Welt!\nBonjour, Monde!", actual);
}

#[test]
pub fn insert_optional_utf16() {
    let table_name = "InsertOptionalUtf16";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(50)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("Hello, World!\nNULL\nHallo, Welt!", actual);
}

#[test]
pub fn insert_bool() {
    let table_name = "InsertBool";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BIT"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1\n0\n0", actual);
}

#[test]
pub fn insert_optional_bool() {
    let table_name = "InsertOptionalBool";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BIT"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1\nNULL\n0", actual);
}

#[test]
pub fn insert_f32() {
    let table_name = "InsertF32";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["FLOAT"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["FLOAT"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1.2000000476837158\nNULL\n3.4000000953674316", actual);
}

#[test]
pub fn insert_f64() {
    let table_name = "InsertF64";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["FLOAT(53)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1.2\n3.3999999999999999\n5.5999999999999996", actual);
}

#[test]
pub fn insert_optional_f64() {
    let table_name = "InsertOptionalF64";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["FLOAT(53)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1.2\nNULL\n3.3999999999999999", actual);
}

#[test]
pub fn insert_date() {
    let table_name = "InsertDate";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DATE"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1970-01-01\n1971-01-01\n2021-03-09", actual);
}

#[test]
pub fn insert_optional_date() {
    let table_name = "InsertOptionalDate";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["Date"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("1970-01-01\nNULL\n2021-03-09", actual);
}

#[test]
pub fn insert_time_ms() {
    let table_name = "InsertTimeMs";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["TIME(3)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("00:00:00.000\n01:00:00.000\n23:00:00.000", actual);
}

#[test]
pub fn insert_optional_time_ms() {
    let table_name = "InsertOptionalTimeMs";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["TIME(3)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("00:00:00.000\nNULL\n23:00:00.000", actual);
}

#[test]
pub fn insert_time_us() {
    let table_name = "InsertTimeUs";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["TIME(6)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("00:00:00.000000\n01:00:00.000000\n23:00:00.000000", actual);
}

#[test]
pub fn insert_optional_time_us() {
    let table_name = "InsertOptionalTimeUs";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["TIME(6)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("00:00:00.000000\nNULL\n23:00:00.000000", actual);
}

#[test]
pub fn insert_decimal_from_i32() {
    let table_name = "InsertDecimalFromI32";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\n1234567.89\n-.42", actual);
}

#[test]
pub fn insert_decimal_from_i32_optional() {
    let table_name = "InsertDecimalFromI32Optional";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\nNULL\n-.42", actual);
}

#[test]
pub fn insert_decimal_from_i64() {
    let table_name = "InsertDecimalFromI64";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\n1234567.89\n-.42", actual);
}

#[test]
pub fn insert_decimal_from_i64_optional() {
    let table_name = "InsertDecimalFromI64Optional";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\nNULL\n-.42", actual);
}

#[test]
pub fn insert_timestamp_ms() {
    let table_name = "InsertTimestampMs";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DATETIME2"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DATETIME2"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DATETIME2"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DATETIME2"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARBINARY(50)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["VARBINARY(50)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BINARY(13)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["BINARY(13)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\n.01\n655.36\n-.01\n-.01", actual);
}

#[test]
pub fn insert_decimal_from_binary_optional() {
    let table_name = "InsertDecimalFromBinaryOptional";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DECIMAL(9,2)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\nNULL\n.01", actual);
}

#[test]
pub fn insert_decimal_from_fixed_binary() {
    let table_name = "InsertDecimalFromFixedBinary";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DECIMAL(5,2)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".00\n.01\n655.36\n-.01", actual);
}

#[test]
pub fn insert_decimal_from_fixed_binary_optional() {
    let table_name = "InsertDecimalFromFixedBinaryOptional";
    // Prepare table
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DECIMAL(5,2)"]).unwrap();

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
        .args(&[
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
    let query = format!("SELECT a FROM {} ORDER BY Id", table_name);
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(".01\n-.01", actual);
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
    T::write_batch(&mut col_writer, values, def_levels);
    row_group_writer.close_column(col_writer).unwrap();
    writer.close_row_group(row_group_writer).unwrap();
    writer.close().unwrap();
}

/// Creates the table and assures it is empty. Columns are named a,b,c, etc.
pub fn setup_empty_table(
    conn: &Connection,
    table_name: &str,
    column_types: &[&str],
) -> Result<(), odbc_api::Error> {
    let drop_table = &format!("DROP TABLE IF EXISTS {}", table_name);

    let column_names = &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"];
    let cols = column_types
        .iter()
        .zip(column_names)
        .map(|(ty, name)| format!("{} {}", name, ty))
        .collect::<Vec<_>>()
        .join(", ");

    let create_table = format!(
        "CREATE TABLE {} (id int IDENTITY(1,1),{});",
        table_name, cols
    );
    conn.execute(&drop_table, ())?;
    conn.execute(&create_table, ())?;
    Ok(())
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {}", table_name), ())
        .unwrap();
    conn.execute(
        &format!(
            "CREATE TABLE {} (country VARCHAR(255), population BIGINT);",
            table_name
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

    let in_path = format!("tests/{}", file);

    // Insert csv
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            &in_path,
            table_name,
        ])
        .assert()
        .success();

    // Query csv
    Command::cargo_bin("odbc2parquet")
        .unwrap()
        .args(&[
            "-vvvv",
            "query",
            "--connection-string",
            MSSQL,
            out_str,
            &format!(
                "SELECT country, population FROM {} ORDER BY population;",
                table_name
            ),
        ])
        .assert();

    let expectation = String::from_utf8(
        std::process::Command::new("parquet-read")
            .arg(in_path)
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap();

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(expectation)
}

/// Consumes a cursor and generates a CSV string from the result set.
fn cursor_to_string(cursor: impl Cursor) -> String {
    let batch_size = 20;
    let mut buffer = TextRowSet::for_cursor(batch_size, &cursor, None).unwrap();
    let mut row_set_cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let mut text = String::new();

    while let Some(row_set) = row_set_cursor.fetch().unwrap() {
        for row_index in 0..row_set.num_rows() {
            if row_index != 0 {
                text.push_str("\n");
            }
            for col_index in 0..row_set.num_cols() {
                if col_index != 0 {
                    text.push_str(",");
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
