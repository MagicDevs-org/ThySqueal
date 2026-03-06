use anyhow::Result;
use rustyline::{Editor, history::DefaultHistory};

pub async fn start() -> Result<()> {
    let mut rl: Editor<(), DefaultHistory> = Editor::new()?;
    
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }

    println!("thy-squeal client v{}", env!("CARGO_PKG_VERSION"));
    println!("Type .help for commands, .quit to exit\n");

    loop {
        let readline = rl.readline("thy> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                rl.add_history_entry(line)?;

                if line == ".quit" || line == ".exit" {
                    break;
                }

                if line == ".help" {
                    print_help();
                    continue;
                }

                if line.starts_with('.') {
                    println!("Unknown command: {}", line);
                    print_help();
                    continue;
                }

                println!("Executing: {}", line);
                println!("(Not implemented yet)");
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    rl.save_history("history.txt")?;
    Ok(())
}

fn print_help() {
    println!("Available commands:");
    println!("  .help     - Show this help");
    println!("  .quit     - Exit the REPL");
    println!("  .exit     - Exit the REPL");
    println!();
    println!("SQL queries can be entered directly.");
}
