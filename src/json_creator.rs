use crate::query::TableMetadata;

use serde_json;
use std::error::Error;

fn create_json_string(data: &[TableMetadata]) -> Result<String, Box<dyn Error>> {
    let json_string = serde_json::to_string_pretty(data)?;
    Ok(json_string)
}

pub fn write_to_json(filename: &str, data: Vec<TableMetadata>) -> Result<(), Box<dyn Error>> {
    let json_string = create_json_string(&data)?;
    std::fs::write(filename, json_string)?;
    Ok(())
}
