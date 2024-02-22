mod cli;
mod csv_ingester;
mod inference;
mod json_creator;
mod processor;
mod query;

use std::error::Error;
use structopt::StructOpt;

use cli::Cli;
use csv_ingester::{read_csv_file, write_to_csv};
use inference::find_similar_tables;
use json_creator::write_to_json;
use query::Query;

const SIMILARITY_THRESHOLD: f64 = 0.8;

fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::from_args();
    let mut reader = read_csv_file(&args.input)?;

    let queries: Result<Vec<Query>, Box<dyn Error>> = csv_ingester::read_query(&mut reader);
    let queries: Vec<Query> = queries?;

    let processed_queries: Result<Vec<processor::Data>, Box<dyn Error>> =
        processor::process_queries(queries);
    let processed_queries: Vec<processor::Data> = processed_queries?;

    let similar_tables = find_similar_tables(&processed_queries, SIMILARITY_THRESHOLD);

    match args.format.as_str() {
        "json" => {
            write_to_json("tables_output.json", &processed_queries)?;
            write_to_json("similar_tables_output.json", &similar_tables.grouped_tables)?;
            println!("Data has been written to json");
        }
        "csv" => {
            write_to_csv("tables_output.csv", processed_queries)?;
            println!("Data has been written to csv");
        }
        _ => {
            eprintln!("Unsupported output format: {}", args.format);
            return Ok(());
        }
    }
    Ok(())
}
