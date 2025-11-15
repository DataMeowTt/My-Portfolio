CREATE TABLE IF NOT EXISTS customer_sql_mapping (
    csv_file_name VARCHAR(100),
    sql_file_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
