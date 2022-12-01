mod cli;
mod dataflow;

use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    ctx.register_csv("example", "example.csv", CsvReadOptions::new())
        .await?;

    // create a plan to run a SQL query
    let df = ctx
        .sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100")
        .await?;

    // execute and print results
    df.show().await?;

    Ok(())
}
