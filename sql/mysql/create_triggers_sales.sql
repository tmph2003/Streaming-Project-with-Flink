CREATE TRIGGER after_insert_transactions
AFTER INSERT ON Transactions
FOR EACH ROW
BEGIN
    -- Thêm dữ liệu vào bảng sales_per_category
    INSERT INTO sales_per_category (transaction_date, category, total_sales)
    VALUES (NEW.transactionDate, NEW.productCategory, NEW.totalAmount)
    ON DUPLICATE KEY UPDATE total_sales = total_sales + NEW.totalAmount;

    -- Thêm dữ liệu vào bảng sales_per_day
    INSERT INTO sales_per_day (transaction_date, total_sales)
    VALUES (NEW.transactionDate, NEW.totalAmount)
    ON DUPLICATE KEY UPDATE total_sales = total_sales + NEW.totalAmount;

    -- Thêm dữ liệu vào bảng sales_per_month
    INSERT INTO sales_per_month (`year`, `month`, total_sales)
    VALUES (YEAR(NEW.transactionDate), MONTH(NEW.transactionDate), NEW.totalAmount)
    ON DUPLICATE KEY UPDATE total_sales = total_sales + NEW.totalAmount;
END;