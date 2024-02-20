use crate::query::TableMetadata;

use std::error::Error;
use std::fs::File;
use std::path::PathBuf;
use csv::{ReaderBuilder, Writer};

pub fn read_csv_file(input_path: &PathBuf) -> Result<csv::Reader<File>, Box<dyn Error>> {
    let file = File::open(input_path)?;
    let reader = ReaderBuilder::new().has_headers(true).from_reader(file);
    Ok(reader)
}

pub fn write_to_csv(filename: &str, data: Vec<TableMetadata>) -> Result<(), Box<dyn Error>> {
    let mut writer = Writer::from_path(filename)?;

    writer.write_record(&["table_name", "columns"])?;

    for metadata in data {
        let columns = metadata.columns.join(", ");
        writer.write_record(&[&metadata.table_name, &columns])?;
    }

    writer.flush()?;
    
    Ok(())
}
