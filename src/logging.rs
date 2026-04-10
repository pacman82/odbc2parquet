use env_logger::Builder;
use log::LevelFilter;

pub fn init(verbosity: usize, no_color: bool) {
    let level = match verbosity {
        0 => LevelFilter::Error,
        1 => LevelFilter::Warn,
        2 => LevelFilter::Info,
        3 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    let write_style = if no_color {
        env_logger::WriteStyle::Never
    } else {
        env_logger::WriteStyle::Auto
    };

    Builder::new()
        .filter_module("odbc2parquet", level)
        .filter_module("odbc_api", level)
        .format_target(false)
        .write_style(write_style)
        .init();
}
