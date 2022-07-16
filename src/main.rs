mod cli;
mod compiler;
mod dataflow;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SQLParser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = cli::Cli::new();

    app.execute();

    Ok(())
}
