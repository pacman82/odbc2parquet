# Contributions

Whether they be in code, interesting feature suggestions, design critique or bug reports, all contributions are welcome. Please start an issue, before investing a lot of work. This helps avoid situations there I would feel the need to reject a large body of work, and a lot of your time has been wasted. `odbc-parquet` is a pet project and a work of love, which implies that I maintain it in my spare time. Please understand that I may not always react immediately. If you contribute code to fix a Bug, please also contribute the test to fix it. Happy contributing.

## Local build and test setup

Running local tests currently requires:

* Docker and Docker compose.

### Visual Studio Code

Should you use Visual Studio Code with the Remote Development extension, it will pick up the `.devcontainer` configuration and everything should be setup for you.

### Not Visual Studio Code

With docker installed run:

```shell
docker-compose up
```

This starts two containers called `odbc2parquet_dev` and `odbc2parquet_mssql`. You can use the `dev` container to build your code and execute tests in case you do not want to install the required ODBC drivers and/or Rust tool chain on your local machine.

Otherwise you can manually install these requirements from here:

* Install Rust compiler and Cargo. Follow the instructions on [this site](https://www.rust-lang.org/en-US/install.html). We need the nightly tool chain.
* [Microsoft ODBC Driver 17 for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15).
* An ODBC Driver manager if you are not on windows: http://www.unixodbc.org/
* The command line tools shipping with the `parquet` crate are invoked by the tests. `cargo +nigthtly install parquet`.

### Workaround

Have not figured out, how to get the nightly tool chain during Docker setup yet. Meanwhile you must use `rustup toolchain install nightly` and `cargo +nightly install parquet` to install the missing pieces for the development setup on the dev container.


We now can execute the tests in Rust typical fashion using:

```
cargo test
```

to run all tests in the workspace.
