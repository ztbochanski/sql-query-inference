use serde::Serialize;
use serde_json;
use std::error::Error;


pub fn create_json_string<T: Serialize>(data: &[T]) -> Result<String, Box<dyn Error>> {
    let json_string = serde_json::to_string_pretty(data)?;
    Ok(json_string)
}

pub fn write_to_json<T: Serialize>(
    filename: &str,
    data: &[T]
) -> Result<(), Box<dyn Error>> {
    let json_string = create_json_string(data)?;
    std::fs::write(filename, json_string)?;
    Ok(())
}
