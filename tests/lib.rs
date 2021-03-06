use assert_cmd::Command;
use lazy_static::lazy_static;
use odbc_api::{Connection, Environment, IntoParameter};
use predicates::ord::eq;
use tempfile::tempdir;

const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
lazy_static! {
    pub static ref ENV: Environment = unsafe { Environment::new().unwrap() };
}

#[test]
fn nullable_parquet_buffers() {
    let expected = "\
        {title: \"Interstellar\", year: null}\n\
        {title: \"2001: A Space Odyssey\", year: 1968}\n\
        {title: \"Jurassic Park\", year: 1993}\n\
    ";

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
            "SELECT title,year from Movies order by year",
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
    let expected = "\
        {title: \"2001: A Space Odyssey\", year: 1968}\n\
    ";

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
            "SELECT title,year from Movies where year=?",
            "1968",
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
    let expected_values = "\
        {day: 2020-09-09 +00:00, time: \"00:05:34\", product: 54, price: 9.99}\n\
        {day: 2020-09-10 +00:00, time: \"12:05:32\", product: 54, price: 9.99}\n\
        {day: 2020-09-10 +00:00, time: \"14:05:32\", product: 34, price: 2.00}\n\
        {day: 2020-09-11 +00:00, time: \"06:05:12\", product: 12, price: -1.50}\n\
    ";

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
            "SELECT day, time, product, price FROM Sales ORDER BY id",
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
    let expected_values = "{\
        my_char: \"abcde\", \
        my_numeric: 0.12, \
        my_decimal: 0.12, \
        my_integer: 42, \
        my_smallint: 42, \
        my_float: 1.23, \
        my_real: 1.23, \
        my_double: 1.23, \
        my_varchar: \"Hello, World!\", \
        my_date: 2020-09-16 +00:00, \
        my_time: \"03:54:12.0000000\", \
        my_timestamp: 2020-09-16 03:54:12 +00:00\
    }\n";

    // A temporary directory, to be removed at the end of the test.
    let out_dir = tempdir().unwrap();
    // The name of the output parquet file we are going to write. Since it is in a temporary
    // directory it will not outlive the end of the test.
    let out_path = out_dir.path().join("out.par");
    // We need to pass the output path as a string argument.
    let out_str = out_path.to_str().expect("Tempfile path must be utf8");

    let query = "SELECT \
        my_char, \
        my_numeric, \
        my_decimal, \
        my_integer, \
        my_smallint, \
        my_float, \
        my_real, \
        my_double, \
        my_varchar, \
        my_date, \
        my_time, \
        my_timestamp \
        FROM AllTheTypes;";

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
            "--batch-size",
            "1",
            "--batches-per-file",
            "1",
            "SELECT title FROM Movies ORDER BY year",
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
    let out_str = out_path.to_str().expect("Tempfile path must be utf8");

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
    let out_str = out_path.to_str().expect("Tempfile path must be utf8");

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
    let out_str = out_path.to_str().expect("Tempfile path must be utf8");

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
    let out_str = out_path.to_str().expect("Tempfile path must be utf8");

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

/// Creates the table and assures it is empty. Columns are named a,b,c, etc.
pub fn setup_empty_table(
    conn: &Connection,
    table_name: &str,
    column_types: &[&str],
) -> Result<(), odbc_api::Error> {
    let drop_table = &format!("DROP TABLE IF EXISTS {}", table_name);

    let column_names = &["a", "b", "c", "d", "e"];
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
