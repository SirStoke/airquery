use clap::Parser;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser as SQLParser, ParserError};
use std::io::Write;
use std::process::exit;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

#[derive(Parser)]
#[clap(author, version, about, name = "airquery", long_about = None)]
struct Args {
    query: String,
    #[clap(env = "AIRQUERY_API_KEY", short = 'k', long)]
    airtable_api_key: String,
    #[clap(env = "AIRQUERY_BASE", short = 'b', long)]
    airtable_base: String,
}

struct Colors {
    error: ColorSpec,
    reset: ColorSpec,
}

pub struct Cli {
    stderr: StandardStream,
    args: Args,
    colors: Colors,
}

/// Entrypoint for the app
impl Cli {
    pub fn new() -> Cli {
        let args = Args::parse();
        let stderr = StandardStream::stderr(ColorChoice::Always);

        let mut colors = Colors {
            error: ColorSpec::new(),
            reset: ColorSpec::new(),
        };

        colors.error.set_fg(Some(Color::Red));
        colors.reset.set_fg(None);

        Cli {
            stderr,
            args,
            colors,
        }
    }

    /// Executes the query
    pub fn execute(&mut self) {
        match SQLParser::parse_sql(&GenericDialect {}, &self.args.query) {
            Err(ParserError::ParserError(err)) => self.quit_because(err),
            Err(ParserError::TokenizerError(err)) => self.quit_because(err),
            Ok(ast) => println!("AST: {:?}", ast),
        }
    }

    /// quits the process because of an error message
    fn quit_because(&mut self, msg: String) -> ! {
        self.stderr.set_color(&self.colors.error).unwrap();
        write!(self.stderr, "Error: ").unwrap();

        self.stderr.set_color(&self.colors.reset).unwrap();
        writeln!(self.stderr, "{}", &msg).unwrap();

        exit(-1)
    }
}
