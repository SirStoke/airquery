mod cli;
mod compiler;
mod dataflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut app = cli::Cli::new();

    app.execute();

    Ok(())
}
