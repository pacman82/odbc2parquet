# Contributions

Whether they be in code, interesting feature suggestions, design critique or bug reports, all contributions are welcome. Please start an issue, before investing a lot of work. This helps avoid situations there I would feel the need to reject a large body of work, and a lot of your time has been wasted. `odbc2parquet` is a pet project and a work of love, which implies that I maintain it in my spare time. Please understand that I may not always react immediately. If you contribute code to fix a Bug, please also contribute the test to fix it. Happy contributing.

## Commit Style

* This repository uses **conventional commits**: <https://www.conventionalcommits.org/en/v1.0.0/>
* This repository has a **linear history**. I.e. no merge commits. We use rebase to merge.
* Every commit should compile and pass the tests.

## Local build and test setup

Running local tests currently requires:

* Docker

Run `docker compose up` to start the various databases used in the integration tests.

There is a devcontainer which will automatically be picked up by Visual Studio Code or Zed. It contains a Rust toolchain and the necessary ODBC drivers to run the integration tests.

If you do not want to use a devcontainer you can install these requirements from here:

* Install Rust compiler and Cargo. Follow the instructions on [this site](https://www.rust-lang.org/en-US/install.html).
* The command line tools shipping with the `parquet` crate are invoked by the tests. `cargo install parquet --features cli`.
* [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16).
* PostgreSQL ODBC driver

Execute

```shell
cargo test
```

to run all tests in the workspace, which should now succeed.
