//! SQL Parser for Graph Database Views
//!
//! This module provides SQL parsing capabilities for materialized views.
//! It uses the sqlparser crate to parse SQL statements and convert them
//! into executable query plans for the graph database.

use sqlparser::ast::{Statement, Query, Select, SetExpr, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

/// SQL parsing errors
#[derive(Debug, Error)]
pub enum SqlParseError {
    #[error("Failed to parse SQL: {0}")]
    ParseError(String),
    
    #[error("Unsupported SQL statement: {0}")]
    UnsupportedStatement(String),
    
    #[error("Invalid query structure: {0}")]
    InvalidQuery(String),
    
    #[error("SQL feature not supported: {0}")]
    UnsupportedFeature(String),
}

/// SQL parser for graph database views
#[derive(Debug)]
pub struct SqlParser {
    dialect: GenericDialect,
}

impl Default for SqlParser {
    fn default() -> Self {
        Self {
            dialect: GenericDialect {},
        }
    }
}

impl SqlParser {
    /// Create a new SQL parser
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Parse a SQL query string into an AST
    pub fn parse(&self, sql: &str) -> Result<Box<Statement>, SqlParseError> {
        let statements = Parser::parse_sql(&self.dialect, sql)
            .map_err(|e| SqlParseError::ParseError(e.to_string()))?;
            
        if statements.is_empty() {
            return Err(SqlParseError::ParseError("Empty SQL statement".to_string()));
        }
        
        if statements.len() > 1 {
            return Err(SqlParseError::ParseError(
                "Multiple statements not supported".to_string()
            ));
        }
        
        Ok(Box::new(statements.into_iter().next().unwrap()))
    }
    
    /// Validate that a SQL statement is supported for materialized views
    pub fn validate_for_view(&self, statement: &Statement) -> Result<(), SqlParseError> {
        match statement {
            Statement::Query(query) => {
                self.validate_query(query)?;
                Ok(())
            }
            Statement::CreateView { .. } => {
                // CREATE VIEW is supported for defining views
                Ok(())
            }
            _ => Err(SqlParseError::UnsupportedStatement(
                format!("{:?}", statement)
            )),
        }
    }
    
    /// Validate a query for materialized view compatibility
    fn validate_query(&self, query: &Query) -> Result<(), SqlParseError> {
        // Check that the query is a SELECT statement
        match query.body.as_ref() {
            SetExpr::Select(select) => {
                self.validate_select(select)?;
            }
            _ => return Err(SqlParseError::UnsupportedFeature(
                "Non-SELECT queries not supported".to_string()
            )),
        }
        
        // Check for unsupported features at query level
        if query.limit.is_some() || query.offset.is_some() {
            return Err(SqlParseError::UnsupportedFeature(
                "LIMIT/OFFSET not yet supported".to_string()
            ));
        }
        
        Ok(())
    }
    
    /// Validate a SELECT statement
    fn validate_select(&self, select: &Select) -> Result<(), SqlParseError> {
        // Check FROM clause for graph tables
        for table in &select.from {
            match &table.relation {
                TableFactor::Table { name, .. } => {
                    let table_name = name.to_string();
                    if !self.is_supported_table(&table_name) {
                        return Err(SqlParseError::UnsupportedFeature(
                            format!("Table '{}' not supported", table_name)
                        ));
                    }
                }
                _ => return Err(SqlParseError::UnsupportedFeature(
                    "Complex FROM clauses not supported".to_string()
                )),
            }
        }
        
        // Check for unsupported features
        if select.distinct.is_some() {
            return Err(SqlParseError::UnsupportedFeature(
                "DISTINCT not yet supported".to_string()
            ));
        }
        
        Ok(())
    }
    
    /// Check if a table name is supported
    fn is_supported_table(&self, table_name: &str) -> bool {
        // Supported graph tables
        matches!(table_name.to_lowercase().as_str(), 
            "vertices" | "edges" | "vertex" | "edge" | "graph")
    }
    
    /// Extract referenced tables from a SQL statement
    pub fn extract_tables(&self, statement: &Statement) -> Vec<String> {
        let mut tables = Vec::new();
        
        match statement {
            Statement::Query(query) => {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    for table in &select.from {
                        if let TableFactor::Table { name, .. } = &table.relation {
                            tables.push(name.to_string());
                        }
                    }
                }
            }
            Statement::CreateView { query, .. } => {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    for table in &select.from {
                        if let TableFactor::Table { name, .. } = &table.relation {
                            tables.push(name.to_string());
                        }
                    }
                }
            }
            _ => {}
        }
        
        tables
    }
    
    /// Create a simple explanation of the SQL statement
    pub fn explain(&self, statement: &Statement) -> String {
        match statement {
            Statement::Query(query) => {
                format!("SELECT query with {} columns", 
                    if let SetExpr::Select(select) = query.body.as_ref() {
                        select.projection.len()
                    } else {
                        0
                    })
            }
            Statement::CreateView { name, .. } => {
                format!("CREATE VIEW {}", name)
            }
            _ => format!("{:?}", statement),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_select() {
        let parser = SqlParser::new();
        let sql = "SELECT * FROM vertices";
        let result = parser.parse(sql);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_validate_select() {
        let parser = SqlParser::new();
        let sql = "SELECT * FROM vertices";
        let stmt = parser.parse(sql).unwrap();
        let result = parser.validate_for_view(&stmt);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_extract_tables() {
        let parser = SqlParser::new();
        let sql = "SELECT * FROM vertices v JOIN edges e ON v.id = e.src";
        let stmt = parser.parse(sql).unwrap();
        let tables = parser.extract_tables(&stmt);
        assert!(tables.iter().any(|t| t.contains("vertices")));
        assert!(tables.iter().any(|t| t.contains("edges")));
    }
}