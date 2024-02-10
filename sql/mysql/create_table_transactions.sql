CREATE TABLE IF NOT EXISTS Transactions (
    transactionId VARCHAR(255) PRIMARY KEY,
    productId VARCHAR(255),
    productName VARCHAR(255),
    productCategory VARCHAR(255),
    productPrice DECIMAL(10, 2),
    productQuantity BIGINT,
    productBrand VARCHAR(255),
    currency VARCHAR(10),
    customerId VARCHAR(255),
    transactionDate DATE,
    paymentMethod VARCHAR(255),
    totalAmount DECIMAL(10, 2)
);