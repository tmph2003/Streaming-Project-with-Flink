CREATE TABLE IF NOT EXISTS sales_per_month (
    year INTEGER,
    month INTEGER,
    total_sales DOUBLE PRECISION,
    PRIMARY KEY (year, month)
);