# sql-query-inference

This tool is designed to infer tables and respective columns from a SQL query and related metadata. 

## Features
1. Infer all tables and columns
2. Discover similar tables

## CLI Usage

With `Cargo`:
```bash
cargo run -- --<option> <value> <csv_to_parse>
```

Example:
```bash
cargo run -- --format json query_analysis_data.csv
```


### Arguments

- `csv_to_parse`: The path to the CSV file to parse

### Options

- `--format` or `-f`: The format of the output (default: `json`)
    - `json`: JSON format
    - `csv`: CSV format
