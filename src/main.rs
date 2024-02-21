mod query;
mod cli;
mod csv_ingester;
mod inference;
mod json_creator;

use std::error::Error;
use std::collections::HashMap;
use structopt::StructOpt;

use cli::Cli;
use csv_ingester::{read_csv_file, write_to_csv};
use inference::find_similar_tables;
use json_creator::write_to_json;

fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::from_args();
    let mut reader = read_csv_file(&args.input)?;
    
    let mut tables_columns_map: Vec<Vec<query::TableMetadata>> = Vec::new();
    let mut counter = 0;
    for result in reader.deserialize::<query::Query>() {
        let query: query::Query = result?;
        let tables_columns: Vec<query::TableMetadata> = query.map_tables_columns();
        tables_columns_map.push(tables_columns);
        counter += 1;
    }

    println!("Number of Queries Processed: {}", counter);


    let mut flattened_map: Vec<query::TableMetadata> = Vec::new();
    for inner_vec in tables_columns_map {
        flattened_map.extend(inner_vec);
    }
    let mut table_columns_map: HashMap<String, Vec<String>> = HashMap::new();

    for metadata in &flattened_map {
        let columns = table_columns_map.entry(metadata.table_name.clone()).or_insert(Vec::new());
        columns.extend(metadata.columns.iter().cloned());
        columns.sort();
        columns.dedup();
    }

    let mut modified_flattened_map: Vec<query::TableMetadata> = Vec::new();
    for (table_name, columns) in table_columns_map {
        modified_flattened_map.push(query::TableMetadata {
            table_name,
            columns,
        });
    }

    modified_flattened_map.sort_by_key(|metadata| metadata.table_name.clone());

    let similar_tables = find_similar_tables(&modified_flattened_map);

    match args.format.as_str() {
        "json" => {
            write_to_json("tables_output.json", &modified_flattened_map)?;
            write_to_json("similar_tables_output.json", &similar_tables.grouped_tables)?;
            println!("Data has been written to json");
        },
        "csv" => {
            write_to_csv("tables_output.csv", modified_flattened_map)?;
            println!("Data has been written to csv");
        },
        _ => {
            eprintln!("Unsupported output format: {}", args.format);
            return Ok(());
        }
    }
    Ok(())
}
