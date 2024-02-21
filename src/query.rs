use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Deserialize)]
pub struct Query {
    pub query_text: String,
    pub tables: String,
    pub select_columns: String,
    pub join_columns: String,
    pub where_columns: String,
    pub agg_columns: String,
}

#[derive(Debug, Serialize)]
pub struct TableMetadata {
    pub table_name: String,
    pub columns: Vec<String>,
}

impl Query {
    fn extract_tables(&self) -> &String {
        &self.tables
    }

    fn tokenize_query(&self) -> Vec<&str> {
        self.query_text
            .split(|c| c == '(' || c == ')' || c == ' ')
            .collect()
    }

    fn table_tokens<'a>(&self, tokens: &'a [&'a str]) -> &'a str {
        let table_index = tokens.iter().position(|&r| r == "INTO").unwrap_or(0);
        let tokens = tokens
            .get(table_index + 1)
            .unwrap_or(&"")
            .trim()
            .split('.')
            .last()
            .unwrap_or(&"");
        tokens
    }

    fn column_tokens<'a>(&self, tokens: &'a [&'a str]) -> Vec<&'a str> {
        let columns_index = tokens.iter().position(|&r| r == "(").unwrap_or(0);
        let from_index = tokens.iter().position(|&r| r == "FROM").unwrap_or(0);

        let mut columns: Vec<&str> = Vec::new();

        for col in &tokens[columns_index + 4..from_index] {
            if *col != "" && *col != "FROM" && !col.contains("INTO") {
                let cleaned_col = col.trim_matches(|c| c == ',' || c == ')');
                columns.push(cleaned_col);
            }
        }
        columns
    }

    fn extract_columns(&self) -> Vec<String> {
        let mut columns = HashSet::new();
        let re_column = Regex::new(r#""([^"]+)""#).unwrap();

        for field in [
            &self.select_columns,
            &self.join_columns,
            &self.where_columns,
            &self.agg_columns,
        ]
        .iter()
        {
            for part in field.split(", ") {
                if let Some(caps) = re_column.captures(part) {
                    if let Some(column_name) = caps.get(1) {
                        columns.insert(column_name.as_str().to_string());
                    }
                }
            }
        }
        columns.into_iter().collect()
    }

    pub fn map_tables_columns(&self) -> Vec<TableMetadata> {
        let tables = self.extract_tables();
        let columns = self.extract_columns();

        let table_names: HashMap<_, _> = tables
            .split(", ")
            .map(|t| {
                let parts: Vec<&str> = t.split(" as ").collect();
                if let Some(table_with_schema) = parts.get(0) {
                    let table_with_schema =
                        table_with_schema.trim_matches(|c| c == '[' || c == '\"');
                    let table_name = table_with_schema
                        .split('.')
                        .last()
                        .unwrap()
                        .trim_matches(|c| c == '\"');
                    let alias = parts
                        .get(1)
                        .map_or("", |a| a.trim_matches(|c| c == '\"' || c == ']'));
                    (table_name, alias)
                } else {
                    ("", "")
                }
            })
            .collect();

        if columns.is_empty() {
            let tokens = self.tokenize_query();
            let table_name_tokens = self
                .table_tokens(&tokens)
                .split(", ")
                .map(|t| t.to_string())
                .collect();
            let column_name_tokens = self
                .column_tokens(&tokens)
                .iter()
                .map(|c| c.to_string())
                .collect();
            let table_info: Vec<TableMetadata> = vec![TableMetadata {
                table_name: table_name_tokens,
                columns: column_name_tokens,
            }];
            return table_info;
        }

        let mut table_columns_map: HashMap<String, Vec<String>> = HashMap::new();
        for column in columns {
            let parts: Vec<&str> = column.split(".").collect();
            let table_name = parts[0];
            let column_name = parts[1];

            let table = table_names
                .keys()
                .find(|&k| table_names[k] == table_name)
                .unwrap_or(&table_name);

            table_columns_map
                .entry(table.to_string())
                .or_insert(Vec::new())
                .push(column_name.to_string());
        }

        let table_info: Vec<TableMetadata> = table_columns_map
            .iter()
            .map(|(k, v)| TableMetadata {
                table_name: k.to_string(),
                columns: v.clone(),
            })
            .collect();
        table_info
    }
}
