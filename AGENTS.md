# AGENTS.md - Developer Guidelines for thy-squeal

## Project Overview

thy-squeal is a SQL parser built with Pest (PEG parser generator). It parses simple SELECT statements using a Pest grammar defined in `src/sql.pest`.

## Build, Test, and Development Commands

### Basic Cargo Commands
```bash
# Build the project
cargo build

# Run the project
cargo run

# Run in release mode
cargo run --release

# Build documentation
cargo doc
```

### Testing
```bash
# Run all tests
cargo test

# Run a single test by name
cargo test <test_name>

# Run tests with output
cargo test -- --nocapture

# Run tests and rebuild on changes (watch mode)
cargo watch -x test
```

### Linting and Formatting
```bash
# Run clippy for linting
cargo clippy

# Run clippy with all warnings (including deny)
cargo clippy -- -D warnings

# Format code
cargo fmt

# Check formatting without making changes
cargo fmt -- --check

# Run both clippy and fmt
cargo fmt && cargo clippy
```

### Other Useful Commands
```bash
# Check for errors without building
cargo check

# Show dependencies
cargo tree

# Update dependencies
cargo update
```

## Code Style Guidelines

### General Principles
- Keep code simple and readable
- Use meaningful variable and function names
- Follow Rust idioms and best practices
- Prefer explicit over implicit

### Imports
- Use absolute paths with `use` for external crates (e.g., `use pest::Parser`)
- Group std imports together, then external crate imports
- Sort imports alphabetically within groups

### Formatting
- Use `cargo fmt` for automatic formatting
- 4 spaces for indentation (Rust default)
- Maximum line length: 100 characters (Rust default)
- Use trailing commas in multi-line expressions

### Types
- Use explicit type annotations for function signatures
- Prefer `&str` over `String` for function parameters when possible
- Use `Result<T, E>` for error handling, avoid `unwrap()` in production code
- Prefer enums over magic numbers or strings

### Naming Conventions
- Variables and functions: `snake_case` (e.g., `parse_sql`, `sql_file`)
- Types and structs: `PascalCase` (e.g., `SqlParser`, `ParseResult`)
- Constants: `SCREAMING_SNAKE_CASE` (e.g., `MAX_BUFFER_SIZE`)
- Files: `snake_case.rs` (e.g., `sql_parser.rs`)

### Error Handling
- Use `Result<T, E>` for functions that can fail
- Use descriptive error messages with `expect()` or `unwrap_or_else()`
- Propagate errors with `?` operator when appropriate
- Match on `Result` types explicitly rather than using `unwrap()`

### Pest Grammar (sql.pest)
- Keep the grammar file in `src/sql.pest`
- Define rules following Pest syntax
- Use `_` prefix for silent rules (whitespace, etc.)
- Document complex rules with comments

### Documentation
- Add docstrings (`///`) to public functions and types
- Document the purpose of each Pest grammar rule
- Keep README.md updated with new features

### Project Structure
```
src/
  main.rs       # Entry point and parser usage
  sql.pest      # Pest grammar definition
examples/
  *.sql         # Example SQL files for testing
```

### Working with Pest
1. Modify grammar in `src/sql.pest`
2. Run `cargo build` to regenerate the parser
3. Test parsing with `cargo run`
4. Use `cargo test` for regression testing

### Testing Strategy
- Add unit tests in the same file as the code they test (using `#[cfg(test)]`)
- Create integration tests in `tests/` directory if needed
- Test both success and error cases
- Use `#[test]` attribute for individual tests
