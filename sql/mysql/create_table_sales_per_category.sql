CREATE TABLE IF NOT EXISTS sales_per_category (
    transaction_date VARCHAR(255),
    category VARCHAR(255),
    total_sales DOUBLE PRECISION,
    PRIMARY KEY (transaction_date, category)
);