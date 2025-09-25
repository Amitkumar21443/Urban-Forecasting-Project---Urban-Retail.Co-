CREATE DATABASE inventory_db;
 drop Database inventory_db; 

USE inventory_db;
CREATE TABLE inventory_forecasting (
    Date DATE,
    Store_ID VARCHAR(10),
    Product_ID VARCHAR(10),
    Category VARCHAR(50),
    Region VARCHAR(50),
    Inventory_Level INT,
    Units_Sold INT,
    Units_Ordered INT,
    Demand_Forecast FLOAT,
    Price FLOAT,
    Discount INT,
    Weather_Condition VARCHAR(50),
    Holiday_Promotion BOOLEAN,
    Competitor_Pricing FLOAT,
    Seasonality VARCHAR(20)
);
SET GLOBAL local_infile = 1;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/inventory_forecasting.csv'
INTO TABLE inventory_forecasting
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select*from inventory_forecasting;



/*1-- Create the products table*/
DROP TABLE IF EXISTS products;

CREATE TABLE products (
    Product_ID VARCHAR(10) PRIMARY KEY,
    Category VARCHAR(50),
    Price FLOAT,
    Seasonality VARCHAR(20)
);

-- Insert distinct product data from inventory_forecasting
INSERT INTO products (Product_ID, Category, Price, Seasonality)
SELECT 
    Product_ID,
    MIN(Category) AS Category,
    MAX(Price) AS Price,
    MAX(Seasonality) AS Seasonality
FROM inventory_forecasting
GROUP BY Product_ID;

select*from products;


-- 2. Store Table (Fixed)
DROP TABLE IF EXISTS stores;

CREATE TABLE stores (
    Store_ID VARCHAR(10) PRIMARY KEY,
    Region VARCHAR(50)
);

INSERT INTO stores (Store_ID, Region)
SELECT 
    Store_ID,
    MAX(Region) AS Region
FROM inventory_forecasting
GROUP BY Store_ID;

SELECT * FROM stores;



-- 3. Inventory_Log Table
DROP TABLE IF EXISTS inventory_log;
CREATE TABLE inventory_log (
    Date DATE,
    Store_ID VARCHAR(10),
    Product_ID VARCHAR(10),
    Inventory_Level INT,
    PRIMARY KEY (Date, Store_ID, Product_ID)
);
INSERT INTO inventory_log (Date, Store_ID, Product_ID, Inventory_Level)
SELECT Date, Store_ID, Product_ID, Inventory_Level
FROM inventory_forecasting;

SELECT * FROM inventory_log;


-- 4. Sales_Log Table
DROP TABLE IF EXISTS sales_log;

CREATE TABLE sales_log (
    Date DATE,
    Store_ID VARCHAR(10),
    Product_ID VARCHAR(10),
    Units_Sold INT,
    PRIMARY KEY (Date, Store_ID, Product_ID)
);

INSERT INTO sales_log (Date, Store_ID, Product_ID, Units_Sold)
SELECT Date, Store_ID, Product_ID, Units_Sold
FROM inventory_forecasting;

SELECT * FROM sales_log;



-- 5. Order_Log Table
DROP TABLE IF EXISTS order_log ;

CREATE TABLE order_log (
    Date DATE,
    Store_ID VARCHAR(10),
    Product_ID VARCHAR(10),
    Units_Ordered INT,
    PRIMARY KEY (Date, Store_ID, Product_ID)
);

INSERT INTO order_log (Date, Store_ID, Product_ID, Units_Ordered)
SELECT Date, Store_ID, Product_ID, Units_Ordered
FROM inventory_forecasting;

SELECT * FROM order_log;



-- 6. Demand_Forecast_Log Table
DROP TABLE IF EXISTS demand_forecast_log;

-- Create table
CREATE TABLE demand_forecast_log (
    Date DATE,
    Store_ID VARCHAR(10),
    Product_ID VARCHAR(10),
    Demand_Forecast FLOAT,
    PRIMARY KEY (Date, Store_ID, Product_ID)
);

-- Insert data (only non-null forecasts)
INSERT INTO demand_forecast_log (Date, Store_ID, Product_ID, Demand_Forecast)
SELECT DISTINCT Date, Store_ID, Product_ID, Demand_Forecast
FROM inventory_forecasting
WHERE Demand_Forecast IS NOT NULL;

-- Verify inserted rows
SELECT COUNT(*) AS row_count FROM demand_forecast_log;

-- Show sample output
SELECT * FROM demand_forecast_log LIMIT 50;




-- 7. Promotion Table
DROP TABLE IF EXISTS promotion;

CREATE TABLE promotion (
    Date DATE,
    Store_ID VARCHAR(10),
    Discount INT,
    Holiday_Promotion BOOLEAN,
    PRIMARY KEY (Date, Store_ID)
);

INSERT INTO promotion (Date, Store_ID, Discount, Holiday_Promotion)
SELECT 
    Date,
    Store_ID,
    MAX(Discount),
    MAX(Holiday_Promotion)
FROM inventory_forecasting
WHERE Discount IS NOT NULL OR Holiday_Promotion IS NOT NULL
GROUP BY Date, Store_ID;
SELECT * FROM promotion LIMIT 150;


-- 8. External_Factors Table
DROP TABLE IF EXISTS external_factors ;

CREATE TABLE external_factors (
    Date DATE,
    Store_ID VARCHAR(10),
    Product_ID VARCHAR(10),
    Weather_Condition VARCHAR(50),
    Competitor_Pricing FLOAT,
    PRIMARY KEY (Date, Store_ID, Product_ID)
);

INSERT INTO external_factors (Date, Store_ID, Product_ID, Weather_Condition, Competitor_Pricing)
SELECT Date, Store_ID, Product_ID, Weather_Condition, Competitor_Pricing
FROM inventory_forecasting;

SELECT * FROM external_factors;



/* inventorylevel across store and products */
SELECT 
    Store_ID,
    Product_ID,
    Category,
    SUM(Inventory_Level) AS Total_Inventory
FROM Inventory_forecasting
GROUP BY Store_ID, Product_ID,Category
ORDER BY Total_Inventory DESC;

/*  Low Inventory Detection Based on Reorder Point*/

SELECT 
    Date,
    Store_ID,
    Product_ID,
    Inventory_Level,
    Category,
    ROUND(AVG(Units_Sold) OVER (PARTITION BY Product_ID), 0) * 1.5 AS Reorder_Point,
    CASE 
        WHEN Inventory_Level < (ROUND(AVG(Units_Sold) OVER (PARTITION BY Product_ID), 0) * 1.5)
        THEN 'REORDER NEEDED'
        ELSE 'OK'
    END AS Reorder_Status
FROM inventory_forecasting;


/*Reorder Point Estimation Using Historical Trends
SELECT 
    Product_ID,
    ROUND(AVG(Units_Sold), 2) AS Avg_Units_Sold,
    ROUND(1.25 * AVG(Units_Sold), 2) AS Suggested_Reorder_Point
FROM inventory_forecasting
GROUP BY Product_ID;*/

SELECT 
    Store_ID,
    Product_ID,
    Category,
    ROUND(AVG(Units_Sold), 2) AS Avg_Units_Sold,
    ROUND(1.25 * AVG(Units_Sold), 2) AS Suggested_Reorder_Point
FROM inventory_forecasting
GROUP BY Store_ID, Product_ID,Category;


/*Inventory Turnover Analysis*/
SELECT 
    `Product_ID`,
    Category,
    ROUND(AVG(`Units_Sold`), 2) AS Avg_Units_Sold,
    ROUND(AVG(`Units_Sold`) * 1.5, 2) AS Suggested_Reorder_Point
FROM inventory_forecasting
GROUP BY `Product_ID`,Category;



 /*Top 20% fast-Moving Products*/

WITH product_sales AS (
    SELECT 
        product_id,
        Category,
       SUM(units_sold) AS total_sold
    FROM inventory_forecasting
    GROUP BY product_id,Category
),
ranked_products AS (
    SELECT 
        ps.product_id,
        ps.total_sold,
       NTILE(5) OVER (ORDER BY ps.total_sold DESC) AS sales_percentile
    FROM product_sales ps
)
SELECT 
    product_id,
    total_sold
FROM ranked_products
WHERE sales_percentile = 1
ORDER BY total_sold DESC;




/*Supplier Performance Report*/
SELECT 
    Product_ID,
    category,
    ROUND(AVG(Units_Sold), 2) AS Avg_Sales,
    ROUND(AVG(Demand_Forecast - Units_Sold), 2) AS Forecast_Error
FROM inventory_forecasting
GROUP BY Product_ID,category
ORDER BY Forecast_Error DESC;



 /*Top 20% Slow-Moving Products*/
	
 WITH product_sales AS (
   SELECT 
 Product_ID,
 Category,
        SUM(Units_Sold) AS Total_Sold
    FROM inventory_forecasting
    GROUP BY Product_ID,Category
),
ranked_products AS (
    SELECT 
        Product_ID,
        Total_Sold,
        NTILE(5) OVER (ORDER BY Total_Sold ASC) AS sales_percentile
    FROM product_sales
)
SELECT 
    Product_ID,
    Total_Sold
FROM ranked_products
WHERE sales_percentile = 1
ORDER BY Total_Sold ASC;




SELECT 
    Date,
    Product_ID,
    Category,
    SUM(Units_Sold) AS Total_Sold,
    SUM(Demand_Forecast) AS Forecast,
    ROUND(SUM(Units_Sold) - SUM(Demand_Forecast), 2) AS Forecast_Error
FROM Inventory_forecasting
GROUP BY Date, Product_ID,Category;



SELECT 
    Date,
    Product_ID,
    Category,
    SUM(Units_Sold) AS Total_Sold,
    ROUND(SUM(Demand_Forecast), 2) AS Forecast,
    ROUND(SUM(Units_Sold) - SUM(Demand_Forecast), 2) AS Forecast_Error,
    ROUND((1 - ABS(SUM(Units_Sold) - SUM(Demand_Forecast)) / NULLIF(SUM(Units_Sold), 0)) * 100, 2) AS Forecast_Accuracy_Percentage
FROM Inventory_forecasting
GROUP BY Date, Product_ID,Category;


SELECT 
    Seasonality,
    AVG(Units_Sold) AS Avg_Sales,
    AVG(Demand_Forecast) AS Avg_Forecast
FROM inventory_forecasting
GROUP BY Seasonality;




/*to Calculate Stockout Rate (with Low Stock Buffer) and
% of times a product had zero inventory but there was demand*/

SELECT 
    COUNT(*) AS stockout_days,
    (SELECT COUNT(*) FROM sales_log) AS total_days,
    ROUND(
        100.0 * COUNT(*) / (SELECT COUNT(*) FROM sales_log), 2
    ) AS stockout_rate_percentage
FROM sales_log s
JOIN inventory_log i
    ON s.Date = i.Date AND s.Store_ID = i.Store_ID AND s.Product_ID = i.Product_ID
WHERE i.Inventory_Level < 5 AND s.Units_Sold > 0;

-- Set total sales records
SET @total_days := (SELECT COUNT(*) FROM sales_log);

/*1. Stockout Rate per Product*/
SELECT 
    i.Product_ID,
    COUNT(CASE WHEN i.Inventory_Level < f.Demand_Forecast THEN 1 END) AS stockout_days,
    COUNT(*) AS total_days,
    ROUND(
        100.0 * COUNT(CASE WHEN i.Inventory_Level < f.Demand_Forecast THEN 1 END) / COUNT(*), 2
    ) AS stockout_rate_percentage
FROM inventory_log i
JOIN demand_forecast_log f
    ON i.Date = f.Date AND i.Store_ID = f.Store_ID AND i.Product_ID = f.Product_ID
GROUP BY i.Product_ID
ORDER BY stockout_rate_percentage DESC;


/*Stockout Rate per Store*/
SELECT 
    i.Store_ID,
    COUNT(CASE WHEN i.Inventory_Level < f.Demand_Forecast THEN 1 END) AS stockout_days,
    COUNT(*) AS total_days,
    ROUND(
        100.0 * COUNT(CASE WHEN i.Inventory_Level < f.Demand_Forecast THEN 1 END) / COUNT(*), 2
    ) AS stockout_rate_percentage
FROM inventory_log i
JOIN demand_forecast_log f
    ON i.Date = f.Date AND i.Store_ID = f.Store_ID AND i.Product_ID = f.Product_ID
GROUP BY i.Store_ID
ORDER BY stockout_rate_percentage DESC;


/*Product × Store × Category Stockout Rate*/
SELECT 
    p.Category,
    i.Product_ID,
    i.Store_ID,
    COUNT(CASE WHEN i.Inventory_Level < f.Demand_Forecast THEN 1 END) AS stockout_days,
    COUNT(*) AS total_days,
    ROUND(
        100.0 * COUNT(CASE WHEN i.Inventory_Level < f.Demand_Forecast THEN 1 END) / COUNT(*), 2
    ) AS stockout_rate_percentage
FROM inventory_log i
JOIN demand_forecast_log f 
    ON i.Date = f.Date AND i.Store_ID = f.Store_ID AND i.Product_ID = f.Product_ID
JOIN products p 
    ON i.Product_ID = p.Product_ID
WHERE p.Category IN ('Toys', 'Clothing', 'Electronics', 'Groceries')
GROUP BY p.Category, i.Product_ID, i.Store_ID
ORDER BY p.Category, stockout_rate_percentage DESC;


-- Combined Stockout Rate by Category, Product, and Store (Clean Final Code)
SELECT 
    p.Category,
    i.Product_ID,
    i.Store_ID,
    COUNT(CASE WHEN i.Inventory_Level < f.Demand_Forecast THEN 1 END) AS stockout_days,
    COUNT(*) AS total_days,
    ROUND(
        100.0 * COUNT(CASE WHEN i.Inventory_Level < f.Demand_Forecast THEN 1 END) / COUNT(*), 2
    ) AS stockout_rate_percentage
FROM inventory_log i
JOIN demand_forecast_log f 
    ON i.Date = f.Date AND i.Store_ID = f.Store_ID AND i.Product_ID = f.Product_ID
JOIN products p 
    ON i.Product_ID = p.Product_ID
-- You can remove the WHERE clause to include all categories
-- WHERE p.Category IN ('Toys', 'Clothing', 'Electronics', 'Groceries')
GROUP BY p.Category, i.Product_ID, i.Store_ID
ORDER BY p.Category, stockout_rate_percentage DESC;



/*Overstock Percentage
% of records where inventory exceeds a threshold (e.g., 1.5× average units sold)*/
SELECT 
    ROUND(
        100.0 * COUNT(*) / (SELECT COUNT(*) FROM inventory_log), 2
    ) AS overstock_percentage
FROM inventory_log inv
JOIN sales_log s
    ON inv.Date = s.Date AND inv.Store_ID = s.Store_ID AND inv.Product_ID = s.Product_ID
JOIN (
    SELECT Product_ID, AVG(Units_Sold) * 1.5 AS overstock_threshold
    FROM sales_log
    GROUP BY Product_ID
) t
    ON inv.Product_ID = t.Product_ID
WHERE inv.Inventory_Level > t.overstock_threshold;


 /*Forecast Error (MAE%)
Mean Absolute Error % between actual and forecasted demand*/
SELECT 
    ROUND(
        100.0 * AVG(ABS(s.Units_Sold - f.Demand_Forecast) / NULLIF(s.Units_Sold, 0)), 2
    ) AS forecast_error_percentage
FROM sales_log s
JOIN demand_forecast_log f
    ON s.Date = f.Date AND s.Store_ID = f.Store_ID AND s.Product_ID = f.Product_ID
WHERE s.Units_Sold > 0;


/*Supplier Performance Gaps
Difference between forecasted demand and actual sales — grouped by product*/
SELECT 
    s.Product_ID,
    ROUND(AVG(f.Demand_Forecast - s.Units_Sold), 2) AS avg_forecast_gap
FROM sales_log s
JOIN demand_forecast_log f
    ON s.Date = f.Date AND s.Store_ID = f.Store_ID AND s.Product_ID = f.Product_ID
GROUP BY s.Product_ID
ORDER BY ABS(avg_forecast_gap) DESC;



/*Total Estimated Revenue Loss or Capital Lock
Lost revenue from stockouts + locked capital in overstock*/
SELECT 
    ROUND(SUM((f.Demand_Forecast - s.Units_Sold) * p.Price), 2) AS estimated_revenue_loss
FROM sales_log s
JOIN demand_forecast_log f
    ON s.Date = f.Date AND s.Store_ID = f.Store_ID AND s.Product_ID = f.Product_ID
JOIN products p
    ON s.Product_ID = p.Product_ID
WHERE f.Demand_Forecast > s.Units_Sold;

SELECT 
    ROUND(SUM(i.Inventory_Level * p.Price), 2) AS estimated_capital_lock
FROM inventory_log i
JOIN products p
    ON i.Product_ID = p.Product_ID
JOIN (
    SELECT Product_ID, AVG(Units_Sold) * 1.5 AS overstock_threshold
    FROM sales_log
    GROUP BY Product_ID
) t
    ON i.Product_ID = t.Product_ID
WHERE i.Inventory_Level > t.overstock_threshold;




SELECT 
    p.Category,
    i.Store_ID,
    COUNT(CASE WHEN i.Inventory_Level < f.Demand_Forecast THEN 1 END) AS stockout_days,
    COUNT(*) AS total_days,
    ROUND(
        100.0 * COUNT(CASE WHEN i.Inventory_Level < f.Demand_Forecast THEN 1 END) / COUNT(*), 2
    ) AS stockout_rate_percentage
FROM inventory_log i
JOIN demand_forecast_log f 
    ON i.Date = f.Date AND i.Store_ID = f.Store_ID AND i.Product_ID = f.Product_ID
JOIN products p 
    ON i.Product_ID = p.Product_ID
WHERE p.Category IN ('Toys', 'Clothing', 'Electronics', 'Groceries')
GROUP BY p.Category, i.Store_ID
ORDER BY p.Category, stockout_rate_percentage DESC;









