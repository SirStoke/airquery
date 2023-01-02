mod cli;
mod dataflow;

use crate::cli::Cli;
use crate::dataflow::connectors::airtable::Airtable;
use datafusion::catalog::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::catalog::schema::{MemorySchemaProvider, SchemaProvider};
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::new();

    let airtable = Airtable::new(
        "Transactions".to_string(),
        cli.args.airtable_base,
        cli.args.airtable_api_key,
    )
    .await?;

    let mut schema_provider = MemorySchemaProvider::new();
    schema_provider.register_table("transactions".to_string(), Arc::new(airtable))?;

    let mut catalog_provider = MemoryCatalogProvider::new();

    catalog_provider.register_schema("airtable", Arc::new(schema_provider))?;

    let ctx = SessionContext::new();

    ctx.register_catalog("airtable", Arc::new(catalog_provider));

    // create a plan to run a SQL query
    let df = ctx
        .sql("SELECT * FROM airtable.airtable.transactions")
        .await?;

    // execute and print results
    df.show().await?;

    Ok(())
}
