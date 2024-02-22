use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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
    pub fn tables(&self) -> Vec<String> {
        let mut tables: Vec<String> = Vec::new();
        if let Ok(table_names) = serde_json::from_str::<Vec<String>>(&self.extract_tables()) {
            tables.extend(table_names);
        } else {
            println!("Failed to parse table names.");
        }
        tables
    }

    pub fn columns(&self) -> Vec<String> {
        self.extract_columns()
    }

    pub fn tables_query_text(&self) -> Vec<String> {
        let tokens = self.tokenize_query();
        let tables = self
            .table_tokens(&tokens)
            .split(", ")
            .map(|t| t.to_string())
            .collect();
        tables
    }

    pub fn columns_query_text(&self) -> Vec<String> {
        let tokens = self.tokenize_query();
        let columns = self
            .column_tokens(&tokens)
            .iter()
            .map(|c| c.to_string())
            .collect();
        columns
    }

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
}
