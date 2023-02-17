mod cli;
mod dataflow;

use crate::cli::Cli;
use crate::dataflow::connectors::airtable::Airtable;
use datafusion::arrow::json::LineDelimitedWriter;
use datafusion::catalog::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::prelude::*;
use futures::StreamExt;
use log::debug;
use std::sync::Arc;
use termcolor::{ColorChoice, StandardStream};

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

    if cli.args.json {
        let mut w = LineDelimitedWriter::new(StandardStream::stdout(
            ColorChoice::Never,
        ));

        let mut stream = df.execute_stream().await?;

        while let Some(Ok(batch)) = stream.next().await {
            for col in batch.columns().iter() {
                debug!("col: {:?}, array: {:?}", col.data_type(), col);
            }

            w.write(batch)?;
        }

        w.finish()?;
    } else {
        // execute and print results
        df.show().await?;
    }

    Ok(())
}
