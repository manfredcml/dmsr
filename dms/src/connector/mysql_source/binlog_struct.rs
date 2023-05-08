use crate::error::{DMSRError, DMSRResult};

#[derive(Debug, Clone)]
pub struct MySQLTableColumn {
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
}

impl MySQLTableColumn {
    pub fn new(
        column_name: String,
        data_type: String,
        is_nullable: bool,
        is_primary_key: bool,
    ) -> Self {
        MySQLTableColumn {
            column_name,
            data_type,
            is_nullable,
            is_primary_key,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MySQLTable {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<MySQLTableColumn>,
}

impl MySQLTable {
    pub fn column_as_mut(&mut self, column_name: &str) -> DMSRResult<&mut MySQLTableColumn> {
        let col = self
            .columns
            .iter_mut()
            .find(|c| c.column_name == column_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                format!("Column {} not found", column_name).into(),
            ))?;
        Ok(col)
    }

    pub fn column_as_ref(&self, column_name: &str) -> DMSRResult<&MySQLTableColumn> {
        let col = self
            .columns
            .iter()
            .find(|c| c.column_name == column_name)
            .ok_or(DMSRError::MySQLSourceConnectorError(
                format!("Column {} not found", column_name).into(),
            ))?;
        Ok(col)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_mysql_table_column() {
        let column = MySQLTableColumn::new("id".to_string(), "INT".to_string(), false, true);
        assert_eq!(column.column_name, "id");
        assert_eq!(column.data_type, "INT");
        assert!(!column.is_nullable);
        assert!(column.is_primary_key);
    }

    #[test]
    fn test_column_as_ref() {
        let columns = vec![
            MySQLTableColumn::new("id".to_string(), "INT".to_string(), false, true),
            MySQLTableColumn::new("name".to_string(), "VARCHAR".to_string(), false, false),
            MySQLTableColumn::new("age".to_string(), "INT".to_string(), true, false),
        ];
        let table = MySQLTable {
            schema_name: "test_schema".to_string(),
            table_name: "test_table".to_string(),
            columns,
        };
        let col_ref = table.column_as_ref("id").unwrap();
        assert_eq!(col_ref.column_name, "id");
        assert_eq!(col_ref.data_type, "INT");
        assert!(!col_ref.is_nullable);
        assert!(col_ref.is_primary_key);
    }

    #[test]
    fn test_column_as_ref_with_non_existent_column() {
        let columns = vec![
            MySQLTableColumn::new("id".to_string(), "INT".to_string(), false, true),
            MySQLTableColumn::new("name".to_string(), "VARCHAR".to_string(), false, false),
            MySQLTableColumn::new("age".to_string(), "INT".to_string(), true, false),
        ];
        let table = MySQLTable {
            schema_name: "test_schema".to_string(),
            table_name: "test_table".to_string(),
            columns,
        };
        let col_ref = table.column_as_ref("non_existent_column");
        assert!(col_ref.is_err());
    }

    #[test]
    fn test_column_as_mut() {
        let columns = vec![
            MySQLTableColumn::new("id".to_string(), "INT".to_string(), false, true),
            MySQLTableColumn::new("name".to_string(), "VARCHAR".to_string(), false, false),
            MySQLTableColumn::new("age".to_string(), "INT".to_string(), true, false),
        ];
        let mut table = MySQLTable {
            schema_name: "test_schema".to_string(),
            table_name: "test_table".to_string(),
            columns,
        };
        let col_ref = table.column_as_mut("name").unwrap();
        col_ref.is_nullable = true;
        assert_eq!(col_ref.column_name, "name");
        assert_eq!(col_ref.data_type, "VARCHAR");
        assert!(col_ref.is_nullable);
        assert!(!col_ref.is_primary_key);
    }

    #[test]
    fn test_column_as_mut_with_non_existent_column() {
        let columns = vec![
            MySQLTableColumn::new("id".to_string(), "INT".to_string(), false, true),
            MySQLTableColumn::new("name".to_string(), "VARCHAR".to_string(), false, false),
            MySQLTableColumn::new("age".to_string(), "INT".to_string(), true, false),
        ];
        let mut table = MySQLTable {
            schema_name: "test_schema".to_string(),
            table_name: "test_table".to_string(),
            columns,
        };
        let col_ref = table.column_as_mut("non_existent_column");
        assert!(col_ref.is_err());
    }
}
