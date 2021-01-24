use assert_cmd::Command;
use predicates::ord::eq;
use tempfile::tempdir;

const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

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
