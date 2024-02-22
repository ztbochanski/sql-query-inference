use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::processor::Data;

#[derive(Debug)]
pub struct SimilarTables {
    pub grouped_tables: Vec<TableGroup>,
}

#[derive(Debug, Serialize)]
pub struct TableGroup {
    pub similar_tables: Vec<String>,
    pub shared_columns: Vec<String>,
    pub similarity_score: String,
}

pub fn find_similar_tables(metadata: &[Data], similarity_threshold: f64) -> SimilarTables {
    let mut grouped_tables: HashMap<Vec<String>, Vec<String>> = HashMap::new();

    for meta in metadata.iter() {
        let mut found_similar = false;
        for (columns, tables) in grouped_tables.iter_mut() {
            let (is_similar, shared_column_percentage) =
                similar(&meta.columns, columns, similarity_threshold);
            if is_similar {
                tables.push(format!(
                    "{}->{}",
                    meta.table_name.clone(),
                    shared_column_percentage
                ));
                found_similar = true;
                break;
            }
        }
        if !found_similar {
            grouped_tables.insert(meta.columns.clone(), vec![meta.table_name.clone()]);
        }
    }

    let grouped_tables_filtered: HashMap<Vec<String>, Vec<String>> = grouped_tables
        .into_iter()
        .filter(|(_, tables)| tables.len() > 1)
        .collect();

    let similar_tables_grouped: Vec<TableGroup> = grouped_tables_filtered
        .into_iter()
        .map(|(columns, tables)| {
            let mut score_total = 0.0;
            for table in &tables {
                let parts = table.split("->").collect::<Vec<&str>>();
                if parts.len() == 2 {
                    let score = parts[1].parse::<f64>().unwrap();
                    score_total += score;
                }
            }
            let similarity_score = format!("{:.2}", score_total / (tables.len() - 1) as f64);
            let tables = tables
                .iter()
                .map(|table| {
                    let mut table_name = table.split("->").collect::<Vec<&str>>()[0].to_string();
                    table_name = table_name.trim().to_string();
                    table_name
                })
                .collect();
            TableGroup {
                similar_tables: tables,
                shared_columns: columns,
                similarity_score,
            }
        })
        .collect();

    SimilarTables {
        grouped_tables: similar_tables_grouped,
    }
}

fn similar(columns1: &[String], columns2: &[String], similarity_threshold: f64) -> (bool, f64) {
    let set1: HashSet<_> = columns1.iter().cloned().collect();
    let set2: HashSet<_> = columns2.iter().cloned().collect();

    let combined_set: HashSet<_> = set1.union(&set2).cloned().collect();
    let num_shared_columns = set1.intersection(&set2).count() as f64;
    let total_unique_columns = (combined_set.len()) as f64;
    let shared_column_percentage: f64 = num_shared_columns / total_unique_columns;
    let is_similar = shared_column_percentage >= similarity_threshold;
    (is_similar, shared_column_percentage)
}
