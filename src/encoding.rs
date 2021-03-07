use structopt::clap::arg_enum;

arg_enum! {
    #[derive(Debug, Clone, Copy)]
    pub enum EncodingArgument {
        System,
        Utf16,
        Auto,
    }
}

impl EncodingArgument {
    pub fn use_utf16(self) -> bool {
        match self {
            EncodingArgument::System => false,
            EncodingArgument::Utf16 => true,
            #[cfg(target_os = "windows")]
            EncodingArgument::Auto => true,
            #[cfg(not(target_os = "windows"))]
            EncodingArgument::Auto => false,
        }
    }
}
