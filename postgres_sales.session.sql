
/*
MILESTONE 3: TASK 1:

#date related columns have already been converted to date type during cleaning with python

Finding out maximum length of characters in columns
 max_length_card_number = 19
 max_length_store_code = 12
 max_length_product_code = 11
 max_length_product_quantity = 2 
 

/* SELECT 
  MAX(LENGTH(CAST(card_number AS TEXT))) AS max_length_card_number,
  MAX(LENGTH(CAST(store_code AS TEXT))) AS max_length_store_code,
  MAX(LENGTH(CAST(product_code AS TEXT))) AS max_length_product_code,
  MAX(LENGTH(CAST(product_quantity AS TEXT))) AS max_length_product_quantity
FROM orders_table;
*/

/*
-- Change data type of 'card_number' column to VARCHAR with a maximum length
ALTER TABLE orders_table
ALTER COLUMN card_number SET DATA TYPE VARCHAR(19); -- Replace 255 with the appropriate maximum length

-- Change data type of 'store_code' column to VARCHAR with a maximum length
ALTER TABLE orders_table
ALTER COLUMN store_code SET DATA TYPE VARCHAR(12); -- Replace 255 with the appropriate maximum length

-- Change data type of 'product_code' column to VARCHAR with a maximum length
ALTER TABLE orders_table
ALTER COLUMN product_code SET DATA TYPE VARCHAR(11); -- Replace 255 with the appropriate maximum length

-- Change data type of 'product_quantity' column to SMALLINT
ALTER TABLE orders_table
ALTER COLUMN product_quantity SET DATA TYPE SMALLINT;

-- Change data type of 'date_uuid' to UUID
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- Change data type of 'user_uuid' to UUID
ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;
*/

---------------------------------------------

/* TASK 2:

join_date & date_of_birth were already converted to 
date/datetime.dt.date in python cleaning

-- Change data types to VARCHAR(255) and VARCHAR(2)

ALTER TABLE dim_users
ALTER COLUMN first_name SET DATA TYPE VARCHAR(255)

ALTER TABLE dim_users
ALTER COLUMN last_name SET DATA TYPE VARCHAR(255)

ALTER TABLE dim_users
ALTER COLUMN country_code SET DATA TYPE VARCHAR(2)

-- Change data type of 'user_uuid' to UUID
ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

*/

---------------------------------------------

/* TASK 3

NO NEED TO MERGE LATITUDE COLUMNS - THE  original/other 'lat' column was dropped after it appeared to contain no actual latitude data 
TASK MENTIONS WEBSITE IN ROW : None existent in orignal uncleaned data. task instructions need updating.

--- store_code and country code, max lengths? 11,2

SELECT 
  MAX(LENGTH(CAST(store_code AS TEXT))) AS max_length_store_code,
  MAX(LENGTH(CAST(country_code AS TEXT))) AS max_length_country_code
FROM dim_store_details;


ALTER TABLE dim_store_details
ALTER COLUMN store_code SET DATA TYPE VARCHAR(11)

ALTER TABLE dim_store_details
ALTER COLUMN country_code SET DATA TYPE VARCHAR(2)

ALTER TABLE dim_store_details
ALTER COLUMN latitude SET DATA TYPE FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN longitude SET DATA TYPE FLOAT;

ALTER TABLE dim_store_details
ALTER COLUMN continent SET DATA TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN locality SET DATA TYPE VARCHAR(255); 

ALTER TABLE dim_store_details
ALTER COLUMN store_type SET DATA TYPE VARCHAR(255); (set data type allows nullable values apparently)

-- Alter 'staff_numbers' column to SMALLINT
ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT;

*/

/* TASK 4: 

no need to remove £ from prices as this was already cleaned up in python cleaning script.

-- Add a new column 'weight_class' to dim_products
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

-- Update 'weight_class' based on the weight ranges
UPDATE dim_products
SET weight_class = 
  CASE 
    WHEN weight_kg < 2 THEN 'Light'
    WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid_Sized'
    WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
    WHEN weight_kg >= 140 THEN 'Truck_Required'
    ELSE NULL  -- Handle any other cases as needed
  END;

------------------------------------------------------------------------------------

TASK 5: 

MAX VALUES: EAN(17), product_code (11), weight_class (14)

ALTER TABLE dim_products
ALTER COLUMN "EAN" SET DATA TYPE VARCHAR(17); 

ALTER TABLE dim_products
ALTER COLUMN product_code SET DATA TYPE VARCHAR(11); 

ALTER TABLE dim_products
ALTER COLUMN weight_class SET DATA TYPE VARCHAR(14); 

product_price and weight_kg already converted to float/double precision and date added is in date format (via python code).

-- Change data type of 'uuid' to UUID
ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;

--- missed the typo in removed column, adjusted in SQL by conversion with typo into BOOL True/False//NULL

-- Rename 'removed' to 'still_available'
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products
ADD COLUMN still_available_bool BOOLEAN;

UPDATE dim_products
SET still_available_bool = CASE 
                             WHEN still_available = 'Still_avaliable' THEN True 
                             WHEN still_available = 'Removed' THEN False 
                             ELSE null  -- handle any other cases if needed
                          END;

ALTER TABLE dim_products
DROP COLUMN still_available;

ALTER TABLE dim_products
RENAME COLUMN still_available_bool TO still_available;

-------------------------------------------------------------------------

TASK 6:

SELECT 
  MAX(LENGTH(CAST(month AS TEXT))) AS max_month,
  MAX(LENGTH(CAST(year AS TEXT))) AS max_year,
  MAX(LENGTH(CAST(time_period AS TEXT))) AS max_time_period,
  MAX(LENGTH(CAST(day AS TEXT))) AS max_day
FROM dim_date_times;

lengths: 2,4,10,2 respectively 

ALTER TABLE dim_date_times
ALTER COLUMN month SET DATA TYPE VARCHAR(2); 

ALTER TABLE dim_date_times
ALTER COLUMN year SET DATA TYPE VARCHAR(4); 

ALTER TABLE dim_date_times
ALTER COLUMN time_period SET DATA TYPE VARCHAR(10); 

ALTER TABLE dim_date_times
ALTER COLUMN day SET DATA TYPE VARCHAR(2); 

-- Change data type of 'date_uuid' to UUID
ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

------------------------------

TASK 7:

SELECT 
  MAX(LENGTH(CAST(card_number AS TEXT))) AS max_cn,
  MAX(LENGTH(CAST(expiry_date AS TEXT))) AS max_ed
FROM dim_card_details;

lengths: 22, 7

ALTER TABLE dim_card_details
ALTER COLUMN card_number SET DATA TYPE VARCHAR(22);

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date SET DATA TYPE VARCHAR(7);

----------------------------------------------------------------

TASK 8: PRIMARY KEYS

-- ADJUST WHERE NEEDED

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);


----------------------------------------------------------------

TASK 9:  FOREIGN KEYS

ADJUST WHERE NEEDED

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code);


----------------------------------------------------------------
----------------------------------------------------------------
----------------------------------------------------------------




/*MILESTONE 4:

TASK 1: number of stores per country_code

SELECT country_code, COUNT(*) AS store_count
FROM dim_store_details
GROUP BY country_code;

----------------------------------------------------------------

TASK 2: Locations with most stores

SELECT locality, COUNT(*) AS store_count
FROM dim_store_details
GROUP BY locality
HAVING COUNT(*) > 0
ORDER BY store_count DESC;

----------------------------------------------------------------

TASK 3: which months produced the most sales?

SELECT
    dt.month AS order_month,
    ROUND(SUM(o.product_quantity * p."product_price_GBP")::NUMERIC, 2) AS total_sales
FROM
    orders_table o
JOIN
    dim_date_times dt ON o.date_uuid = dt.date_uuid
JOIN
    dim_products p ON o.product_code = p.product_code
GROUP BY
    order_month
ORDER BY
    total_sales DESC;

----------------------------------------------------------------    

TASK 4:  show total online/offline number of sales and total_product_quantity sold

SELECT
    CASE
        WHEN o.store_code = 'WEB-1388012W' THEN 'Web'
        ELSE 'Offline'
    END AS purchase_location,
    COUNT(*) AS number_of_sales,
    SUM(o.product_quantity) AS total_product_quantity
FROM
    orders_table o
JOIN
    dim_products p ON o.product_code = p.product_code
GROUP BY
    purchase_location
ORDER BY
    purchase_location;

----------------------------------------------------------------

TASK 5: PROBLEM: Web portal store_type does not exist in the dim_store_details table 
(even before cleaning/ from extraction)

SELECT
    COALESCE(sd.store_type, 'Web Portal') AS purchase_location,
    ROUND(SUM(o.product_quantity * p."product_price_GBP")::NUMERIC, 2) AS total_sales
FROM
    orders_table o
LEFT JOIN
    dim_store_details sd ON o.store_code = sd.store_code
JOIN
    dim_products p ON o.product_code = p.product_code
GROUP BY
    purchase_location
ORDER BY
    total_sales DESC;

----------------------------------------------------------------

TASK 6: Find which months in which years have had the most sales historically.

WITH MonthlySales AS (
    SELECT
        CAST(year AS INTEGER) AS order_year,
        CAST(month AS INTEGER) AS order_month,
        ROUND(SUM(o.product_quantity * p."product_price_GBP")::NUMERIC, 2) AS total_sales
    FROM
        orders_table o
    JOIN
        dim_date_times dt ON o.date_uuid = dt.date_uuid
    JOIN
        dim_products p ON o.product_code = p.product_code
    GROUP BY
        order_year, order_month
)
SELECT
    order_year,
    order_month,
    total_sales
FROM
    MonthlySales
WHERE
    (order_year, total_sales) IN (
        SELECT
            order_year,
            MAX(total_sales) AS max_sales
        FROM
            MonthlySales
        GROUP BY
            order_year
    )
ORDER BY
    total_sales DESC;

----------------------------------------------------------------

TASK 7: Return total staff_numbers per country code:

SELECT
    country_code,
    SUM(staff_numbers) AS total_staff_numbers
FROM
    dim_store_details
GROUP BY
    country_code;

----------------------------------------------------------------

TASK 8: show sales performance of stores in DE, grouped by their store type

SELECT
	dsd.country_code,
    dsd.store_type,
    ROUND(SUM(o.product_quantity * dp."product_price_GBP")::NUMERIC, 2) AS total_sales
FROM
    orders_table o
JOIN
    dim_products dp ON o.product_code = dp.product_code
JOIN
    dim_store_details dsd ON o.store_code = dsd.store_code
WHERE
    dsd.country_code = 'DE'
GROUP BY
   dsd.country_code, dsd.store_type;

----------------------------------------------------------------

TASK 9: AVERAGE TIME BETWEEN ORDERS PER YEAR

SELECT
    order_year,
    '{"hours": ' ||
    (AVG(EXTRACT(EPOCH FROM time_difference)) / 3600)::int || 
    ', "minutes": ' || 
    ((AVG(EXTRACT(EPOCH FROM time_difference)) % 3600) / 60)::int || 
    ', "seconds": ' || 
    ((AVG(EXTRACT(EPOCH FROM time_difference)) % 3600) % 60)::int ||
    '}' AS actual_time_taken
FROM (
    SELECT
        EXTRACT(YEAR FROM dt.datetime) AS order_year,
        dt.datetime,
        LEAD(dt.datetime) OVER (PARTITION BY EXTRACT(YEAR FROM dt.datetime) ORDER BY dt.datetime) - dt.datetime AS time_difference
    FROM
        orders_table o
    JOIN
        dim_date_times dt ON o.date_uuid = dt.date_uuid
) AS subquery
GROUP BY
    order_year
ORDER BY
    order_year;
