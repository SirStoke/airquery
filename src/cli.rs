use clap::Parser;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream};

#[derive(Parser)]
#[clap(author, version, about, name = "airquery", long_about = None)]
pub struct Args {
    pub query: String,
    #[clap(env = "AIRQUERY_API_KEY", short = 'k', long)]
    pub airtable_api_key: String,
    #[clap(env = "AIRQUERY_BASE", short = 'b', long)]
    pub airtable_base: String,
}

pub struct Colors {
    pub error: ColorSpec,
    pub reset: ColorSpec,
}

pub struct Cli {
    pub stderr: StandardStream,
    pub args: Args,
    pub colors: Colors,
}

/// Entrypoint for the app
impl Cli {
    pub fn new() -> Cli {
        let args = Args::parse();
        let stderr = StandardStream::stderr(ColorChoice::Always);

        let mut colors =
            Colors { error: ColorSpec::new(), reset: ColorSpec::new() };

        colors.error.set_fg(Some(Color::Red));
        colors.reset.set_fg(None);

        Cli { stderr, args, colors }
    }
}
