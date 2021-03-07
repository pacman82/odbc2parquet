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
    /// Transalte the command line option to a boolean indicating wether or not wide character
    /// buffers, should be bound.
    pub fn use_utf16(self) -> bool {
        match self {
            EncodingArgument::System => false,
            EncodingArgument::Utf16 => true,
            // Most windows systems do not utilize UTF-8 as their default encoding, yet.
            #[cfg(target_os = "windows")]
            EncodingArgument::Auto => true,
            // UTF-8 is the default on Linux and OS-X.
            #[cfg(not(target_os = "windows"))]
            EncodingArgument::Auto => false,
        }
    }
}
