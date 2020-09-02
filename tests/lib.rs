use assert_cmd::Command;
use predicates::ord::eq;
use tempfile::tempdir;

#[test]
fn test_xls_table() {
    let expected = "\
    {Text: \"Hello\", Real: 1.3}\n\
    {Text: \"World\", Real: 5.4}\n\
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
    cmd.args(&["-vvvv", "foobar", "SELECT * FROM [uk-500$]", out_str])
        .assert()
        .failure()
        .code(1);
}
