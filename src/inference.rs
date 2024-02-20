use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::query::TableMetadata;

#[derive(Debug)]
pub struct SimilarTables {
    pub grouped_tables: Vec<TableGroup>,
}

#[derive(Debug, Serialize)]
pub struct TableGroup {
    pub similar_tables: Vec<String>,
    pub grouped_by: Vec<String>,
}

pub fn find_similar_tables(metadata: &[TableMetadata]) -> SimilarTables {
    let mut grouped_tables: HashMap<Vec<String>, Vec<String>> = HashMap::new();

    for meta in metadata.iter() {
        let mut found_similar = false;
        for (columns, tables) in grouped_tables.iter_mut() {
            if is_similar(&meta.columns, columns) {
                tables.push(meta.table_name.clone());
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
        .map(|(columns, tables)| TableGroup {
            similar_tables: tables,
            grouped_by: columns,
        })
        .collect();

    SimilarTables {
        grouped_tables: similar_tables_grouped,
    }
}

fn is_similar(columns1: &[String], columns2: &[String]) -> bool {
    let set1: HashSet<_> = columns1.iter().cloned().collect();
    let set2: HashSet<_> = columns2.iter().cloned().collect();

    let intersection_count = set1.intersection(&set2).count();
    let threshold = (set1.len() + set2.len()) / 2;

    intersection_count >= threshold
}
