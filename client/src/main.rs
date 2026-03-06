use clap::{Parser, Subcommand};
use anyhow::Result;
use tracing::info;

mod config;
mod http;
mod repl;

#[derive(Parser)]
#[command(name = "thy-squeal-client")]
#[command(about = " thy-squeal SQL client with JavaScript REPL", long_about = None)]
struct Cli {
    #[arg(short, long, default_value = "localhost")]
    host: String,
    
    #[arg(short, long, default_value_t = 3306)]
    port: u16,
    
    #[arg(long)]
    http: bool,
    
    #[arg(short, long)]
    execute: Option<String>,
    
    #[arg(long)]
    import: Option<String>,
    
    #[arg(long)]
    export: Option<String>,
    
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Repl,
    Query { sql: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Some(query) = cli.execute {
        if cli.http {
            http::execute_query(&cli.host, cli.port, &query).await?;
        } else {
            println!("SQL mode: {}", query);
            println!("(TCP client not implemented yet)");
        }
        return Ok(());
    }

    if let Some(file) = cli.import {
        println!("Import from: {}", file);
        println!("(Import not implemented yet)");
        return Ok(());
    }

    if let Some(file) = cli.export {
        println!("Export to: {}", file);
        println!("(Export not implemented yet)");
        return Ok(());
    }

    match &cli.command {
        Some(Commands::Repl) | None => {
            info!("Starting REPL...");
            repl::start().await?;
        }
        Some(Commands::Query { sql }) => {
            println!("Query: {}", sql);
        }
    }

    Ok(())
}
