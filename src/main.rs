mod dataflow;

use clap::Parser;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SQLParser;

#[derive(Parser)]
#[clap(author, version, about, name = "Airquery", long_about = None)]
struct Args {
    query: String,
    #[clap(env = "AIRQUERY_API_KEY", short = 'k', long)]
    airtable_api_key: String,
    #[clap(env = "AIRQUERY_BASE", short = 'b', long)]
    airtable_base: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Args::parse();

    let records = dataflow::connectors::airtable::records(
        "Transactions",
        &cli.airtable_base,
        &cli.airtable_api_key,
        100,
        0,
    )
    .await?;

    println!("{:?}", records);

    let ast = SQLParser::parse_sql(&GenericDialect {}, &cli.query);

    println!("AST: {:?}", ast);

    Ok(())
}
