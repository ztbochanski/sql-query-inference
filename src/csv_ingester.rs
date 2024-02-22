use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

use csv::{Reader, ReaderBuilder, Writer};

use crate::processor::Data;
use crate::query::Query;

pub fn read_csv_file(input_path: &PathBuf) -> Result<csv::Reader<File>, Box<dyn Error>> {
    let file = File::open(input_path)?;
    let reader = ReaderBuilder::new().has_headers(true).from_reader(file);
    Ok(reader)
}

pub fn write_to_csv(filename: &str, data: Vec<Data>) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_path(filename)?;

    writer.write_record(&["table_name", "columns"])?;

    for metadata in data {
        let columns = metadata.columns.join(", ");
        writer.write_record(&[&metadata.table_name, &columns])?;
    }

    writer.flush()?;

    Ok(())
}

pub fn read_query(reader: &mut Reader<File>) -> Result<Vec<Query>, Box<dyn Error>> {
    let mut counter: i32 = 0;
    let mut queries: Vec<Query> = Vec::new();
    for result in reader.deserialize::<Query>() {
        let query: Query = result?;
        queries.push(query);
        counter += 1;
    }
    println!("Number of Queries Read: {}", counter);
    Ok(queries)
}
