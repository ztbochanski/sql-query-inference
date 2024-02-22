use std::collections::HashMap;
use std::error::Error;

use serde::Serialize;

use crate::query::Query;

#[derive(Debug, Serialize)]
pub struct Data {
    pub table_name: String,
    pub columns: Vec<String>,
}

pub fn process_queries(queries: Vec<Query>) -> Result<Vec<Data>, Box<dyn Error>> {
    let mut namespaced_columns: Vec<String> = Vec::new();
    let mut counter: i32 = 0;
    for query in queries {
        if query.columns().is_empty() {
            let tables: Vec<String> = query.tables_query_text();
            let columns: Vec<String> = query.columns_query_text();
            for column in columns {
                namespaced_columns.push(format!("{}.{}", tables[0], column));
            }
        }
        let tables: Vec<String> = query.tables();
        let columns: Vec<String> = query.columns();

        let aliases = table_name_aliases(tables.clone());
        let corrected_columns = replace_aliases(columns.clone(), &aliases);
        for column in corrected_columns {
            namespaced_columns.push(column);
        }
        counter += 1;
    }
    println!("Number of Queries Processed: {}", counter);

    let mut table_columns_map: HashMap<String, Vec<String>> = HashMap::new();
    for column in namespaced_columns {
        let parts: Vec<&str> = column.split(".").collect();
        let table = parts[0];
        let column_name = parts[1];
        table_columns_map
            .entry(table.to_string())
            .or_insert(Vec::new())
            .push(column_name.to_string());
    }

    table_columns_map.iter_mut().for_each(|(_, columns)| {
        columns.sort();
        columns.dedup();
    });

    let mut sorted_keys = table_columns_map.keys().collect::<Vec<&String>>();
    sorted_keys.sort();

    let mut processed_queries: Vec<Data> = Vec::new();
    for key in sorted_keys {
        let columns = table_columns_map.get(key).unwrap();
        let data: Data = Data {
            table_name: key.to_string(),
            columns: columns.to_vec(),
        };
        processed_queries.push(data);
    }

    Ok(processed_queries)
}

fn table_name_aliases(tables: Vec<String>) -> HashMap<String, String> {
    let mut table_name_aliases: HashMap<String, String> = HashMap::new();
    for table in tables {
        let parts: Vec<&str> = table.split(" as ").collect();
        let table_with_schema: &str = parts[0].trim_matches(|c: char| c == '[' || c == '\"');
        let table_name: &str = table_with_schema
            .split('.')
            .last()
            .unwrap()
            .trim_matches(|c: char| c == '\"');
        let alias = parts
            .get(1)
            .map_or("", |a: &&str| {
                a.trim_matches(|c: char| c == '\"' || c == ']')
            })
            .to_string();
        table_name_aliases.insert(table_name.to_string(), alias);
    }
    table_name_aliases
}

fn replace_aliases(columns: Vec<String>, table_aliases: &HashMap<String, String>) -> Vec<String> {
    let mut replaced_columns: Vec<String> = Vec::new();
    for (key, value) in table_aliases {
        for column in &columns {
            let parts: Vec<&str> = column.split(".").collect();
            let table = parts[0];
            let column_name = parts[1];
            if key == table {
                replaced_columns.push(format!("{}.{}", key, column_name));
            } else if value == table {
                replaced_columns.push(format!("{}.{}", key, column_name));
            }
        }
    }
    replaced_columns
}
