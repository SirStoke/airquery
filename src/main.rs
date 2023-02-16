mod cli;
mod dataflow;

use crate::cli::Cli;
use crate::dataflow::connectors::airtable::Airtable;
use datafusion::catalog::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if cfg!(debug_assertions) {
        // We ignore the error if a .env file is not present
        let _ = dotenvy::dotenv();
    }

    simple_logger::init_with_env().unwrap();

    let cli = Cli::new();

    let airtable =
        Airtable::new(cli.args.airtable_base, cli.args.airtable_api_key)
            .await?;

    let catalog_provider = MemoryCatalogProvider::new();

    catalog_provider
        .register_schema("airtable", airtable.schema_provider()?)?;

    let ctx = SessionContext::new();

    ctx.register_catalog("airtable", Arc::new(catalog_provider));

    // create a plan to run a SQL query
    let df = ctx.sql(&cli.args.query).await?;

    // execute and print results
    df.show().await?;

    Ok(())
}
