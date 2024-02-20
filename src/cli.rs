use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt)]
pub struct Cli {
    #[structopt(parse(from_os_str))]
    pub input: PathBuf,

    #[structopt(short = "f", long = "format", default_value = "json")]
    pub format: String,
}
