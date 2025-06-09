{\rtf1\ansi\ansicpg1252\cocoartf2580
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 USE ROLE accountadmin;\
\
USE WAREHOUSE compute_wh;\
\
CREATE OR REPLACE DATABASE CUSTOMER_DATA;\
\
CREATE OR REPLACE SCHEMA RAW_DATA2;\
\
CREATE OR REPLACE SCHEMA FLATTEN_DATA;\
\
USE DATABASE CUSTOMER_DATA;\
\
USE SCHEMA RAW_DATA2;\
\
CREATE OR REPLACE TABLE CUSTOMER_RAW\
(\
DATA VARIANT\
);\
\
USE SCHEMA FLATTEN_DATA;\
\
CREATE OR REPLACE TABLE CUSTOMER_FLATTEN \
(\
CUSTOMERID INT,\
NAME STRING,\
EMAIL STRING,\
REGION STRING,\
COUNTRY STRING,\
PRODUCTNAME STRING,\
PRODUCTBRAND STRING,\
CATEGORY STRING,\
QUANTITY INT,\
PRICEPERUNIT FLOAT,\
TOTALSALES FLOAT,\
PURCHASEMODE STRING,\
MODEOFPAYMENT STRING,\
PURCHASEDATE DATE\
);\
\
CREATE OR REPLACE STAGE CUSTOMER_STAGE;\
\
ALTER TABLE CUSTOMER_RAW SET DATA_RETENTION_TIME_IN_DAYS = 90;\
\
ALTER TABLE CUSTOMER_FLATTEN SET DATA_RETENTION_TIME_IN_DAYS = 90;\
\
\
CREATE OR REPLACE FILE FORMAT my_json_fmt\
  TYPE = 'JSON'\
  STRIP_OUTER_ARRAY = TRUE\
  COMPRESSION = 'AUTO';\
\
-- 2) reload\
TRUNCATE TABLE RAW_DATA2.CUSTOMER_RAW;\
COPY INTO RAW_DATA2.CUSTOMER_RAW\
  FROM @CUSTOMER_STAGE/customer_raw_final.json\
  FILE_FORMAT = (FORMAT_NAME = 'my_json_fmt');\
\
\
SELECT * FROM CUSTOMER_RAW;\
\
CREATE OR REPLACE TABLE FLATTEN_DATA.CUSTOMER_FLATTEN AS\
SELECT \
data:customerid::integer AS customer_id,\
data:name::string AS name,\
data:email::string AS email,\
data:region::string AS region,\
data:country::string AS country,\
data:productname::string AS product_name,\
data:productbrand::string AS product_brand,\
data:category::string AS category,\
data:quantity::integer AS quantity,\
data:priceperunit::float AS price_per_unit,\
data:totalsales::float AS total_sales,\
data:purchasemode::string AS purchase_mode,\
data:modeofpayment::string AS mode_of_payment,\
data:purchasedate::date AS purchase_date\
FROM CUSTOMER_RAW; \
\
SELECT * FROM CUSTOMER_FLATTEN; \
\
-- Calculate the total sales for each region.\
SELECT REGION,\
    SUM(TOTAL_SALES) AS SALES_BY_REGION\
FROM CUSTOMER_FLATTEN\
GROUP BY REGION\
ORDER BY SALES_BY_REGION;\
\
"""\
REGION	SALES_BY_REGION\
South America	 35726.45\
Africa	         43892.96    \
Asia	         45760.73\
North America	 57164.23\
Europe	         64520.1\
Australia	     73604.99\
"""\
-- Identify the region with the highest total sales.\
WITH cte AS(\
    SELECT REGION,\
        SUM(TOTAL_SALES) AS SALES_BY_REGION,\
        RANK() OVER(ORDER BY SUM(TOTAL_SALES) DESC) AS rnk\
    FROM CUSTOMER_FLATTEN\
    GROUP BY REGION\
    ORDER BY SALES_BY_REGION\
)\
SELECT *\
FROM cte \
WHERE rnk = 1;\
-- Australia	73604.99\
\
-- Determine the total quantity sold for each product brand.\
SELECT PRODUCT_BRAND,\
    SUM(quantity) AS QUANTITY_SOLD\
FROM CUSTOMER_FLATTEN\
GROUP BY PRODUCT_BRAND\
ORDER BY QUANTITY_SOLD DESC;\
"""\
PRODUCT_BRAND	QUANTITY_SOLD\
DJI	       44\
Apple	   39\
Canon	   35\
Samsung	   28\
LG	       27\
Microsoft  27\
Garmin	   26\
JBL	       26\
Sony	   25\
Dell	   20\
"""\
\
-- Find the product with the least quantity sold.\
WITH cte AS(\
    SELECT PRODUCT_BRAND,\
        SUM(quantity) AS QUANTITY_SOLD,\
        RANK() OVER(ORDER BY SUM(quantity) DESC) AS rnk\
    FROM CUSTOMER_FLATTEN\
    GROUP BY PRODUCT_BRAND\
    ORDER BY QUANTITY_SOLD DESC    \
) \
SELECT *\
FROM cte\
WHERE rnk = 1;\
-- PRODUCT_BRAND	QUANTITY_SOLD	RNK\
-- DJI	44	1\
\
-- Identify the customer who made the highest purchase.\
WITH cte AS(\
    SELECT CUSTOMER_ID,\
        NAME,\
        TOTAL_SALES,\
        RANK() OVER(ORDER BY TOTAL_SALES DESC) AS rnk\
    FROM CUSTOMER_FLATTEN\
)\
SELECT *\
FROM cte\
WHERE rnk = 1;\
-- CUSTOMER_ID	NAME	TOTAL_SALES	RNK\
-- 56	Makayla Griffin	9805.85	1\
\
-- Locate the product name and brand with the lowest unit price.\
WITH cte AS(\
    SELECT product_name,\
        product_brand,\
        price_per_unit,\
        RANK() OVER(ORDER BY price_per_unit) AS rnk\
    FROM CUSTOMER_FLATTEN\
)\
SELECT *\
FROM cte\
WHERE rnk = 1;\
-- PRODUCT_NAME	PRODUCT_BRAND	PRICE_PER_UNIT	RNK\
-- Gaming Console	Sony	103.49	1\
\
-- List the top 5 best-selling products.\
WITH cte AS(\
    SELECT product_name,\
        SUM(total_sales) AS total_sales,\
        RANK() OVER(ORDER BY SUM(total_sales) DESC) AS rnk\
    FROM CUSTOMER_FLATTEN\
    GROUP BY product_name\
)\
SELECT *\
FROM cte\
WHERE rnk = 1;\
-- PRODUCT_NAME	TOTAL_SALES	RNK\
-- Wireless Earbuds	46568.78	1\
\
-- Identify the 5 countries with the lowest sales.\
WITH cte AS(\
    SELECT COUNTRY,\
        SUM(TOTAL_SALES) AS total_sales,\
        RANK() OVER(ORDER BY SUM(TOTAL_SALES)) AS rnk\
    FROM CUSTOMER_FLATTEN\
    GROUP BY COUNTRY\
)\
SELECT *\
FROM cte\
WHERE rnk <= 5;\
"""\
COUNTRY	TOTAL_SALES	RNK\
Armenia	        283.04	1\
Guadeloupe	    298.6	2\
Hungary	        413.96	3\
Turkmenistan	474.86	4\
Saudi Arabia	482.54	5\
"""\
\
\
\
\
}