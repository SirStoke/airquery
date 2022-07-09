mod dataflow;

use clap::Parser;

#[derive(Parser)]
#[clap(author, version, about, name = "Airquery", long_about = None)]
struct Args {
    #[clap(env = "AIRQUERY_API_KEY")]
    airtable_api_key: String,
    #[clap(env = "AIRQUERY_BASE")]
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

    Ok(())
}
