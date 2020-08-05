use assert_cmd::Command;

#[test]
fn sample_employee_data() {
    // See: https://www.connectionstrings.com/microsoft-excel-odbc-driver/

    let mut cmd = Command::cargo_bin("odbc2parquet").unwrap();
    cmd.args(&[
        "-vvvv",
        "-s",
        "Driver={Microsoft Excel Driver (*.xls)};Dbq=tests/Sample_Employee_data_xls.xls;ReadOnly=0",
        "-q",
        "SELECT * FROM [uk-500$]",
    ])
    .assert()
    .success();
}

#[test]
fn sample_sales_records() {
    // See: https://www.connectionstrings.com/microsoft-excel-odbc-driver/

    let mut cmd = Command::cargo_bin("odbc2parquet").unwrap();
    cmd.args(&[
        "-vvvv",
        "-s",
        "Driver={Microsoft Excel Driver (*.xls)};Dbq=tests/Sample_Sales_Records_xls.xls;ReadOnly=0",
        "-q",
        "SELECT * FROM [Sample_Sales Records$]",
    ])
    .assert()
    .success();
}

#[test]
fn foobar_connection_string() {
    let mut cmd = Command::cargo_bin("odbc2parquet").unwrap();
    cmd.args(&["-vvvv", "-s", "foobar", "-q", "SELECT * FROM [uk-500$]"])
        .assert()
        .failure()
        .code(1);
}
