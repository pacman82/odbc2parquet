use anyhow::Error;

use crate::ExecOpt;

pub fn execute(exec_opt: &ExecOpt) -> Result<(), Error> {
    let ExecOpt {
        connect_opts,
        encoding,
        input,
        statement,
    } = exec_opt;

    

    Ok(())
}