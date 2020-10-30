use assert_cmd::Command;
use predicates::ord::eq;
use tempfile::tempdir;

#[test]
fn test_xls_table() {
    let expected = "\
        {Text: \"Hello\", Real: 1.3, Boolean: true}\n\
        {Text: \"World\", Real: 5.4, Boolean: false}\n\
    ";

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
        "-c",
        // See: https://www.connectionstrings.com/microsoft-excel-odbc-driver/
        "Driver={Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)};Dbq=tests/test-table.xlsx;",
        "SELECT * FROM [sheet1$]",
        out_str,
    ])
    .assert()
    .success();

    // Use the parquet-read tool to verify the output. It can be installed with
    // `cargo install parquet`.
    let mut cmd = Command::new("parquet-read");
    cmd.arg(out_str).assert().success().stdout(eq(expected));
}

/// Currently this test requires the docker setup from `odbc-api` to run.
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
            "--connection-string",
            "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;",
            "SELECT title,year from Movies order by year",
            out_str
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
    cmd.args(&["-vvvv", "-c", "foobar", "SELECT * FROM [uk-500$]", out_str])
        .assert()
        .failure()
        .code(1);
}
