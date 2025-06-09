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
CREATE OR REPLACE DATABASE SALES_DATA;\
\
CREATE OR REPLACE SCHEMA RAW_DATA;\
\
CREATE OR REPLACE SCHEMA FLATTEN_DATA;\
\
USE DATABASE SALES_DATA;\
\
USE SCHEMA RAW_DATA;\
\
CREATE OR REPLACE TABLE SALES_RAW\
(\
DATA VARIANT\
);\
\
USE SCHEMA FLATTEN_DATA;\
\
CREATE OR REPLACE TABLE SALES_FLATTEN\
(\
COMPANIES STRING,\
SALES_PERIOD STRING,\
TOTAL_REVENUE FLOAT,\
TOTAL_UNITS_SOLD FLOAT,\
REGIONS STRING,\
TOTAL_SALES FLOAT,\
PRODUCTS STRING,\
UNITS_SOLD FLOAT,\
REVENUE FLOAT\
);\
\
CREATE OR REPLACE STAGE SALES_STAGE;\
\
ALTER TABLE SALES_RAW SET DATA_RETENTION_TIME_IN_DAYS = 90;\
\
ALTER TABLE SALES_FLATTEN SET DATA_RETENTION_TIME_IN_DAYS = 90;\
\
CREATE OR REPLACE FILE FORMAT my_json_fmt\
  TYPE = 'JSON'\
  STRIP_OUTER_ARRAY = TRUE\
  COMPRESSION = 'AUTO';\
\
-- 2) reload\
TRUNCATE TABLE RAW_DATA.SALES_RAW;\
COPY INTO RAW_DATA.SALES_RAW\
  FROM @SALES_STAGE/sales_data.json\
  FILE_FORMAT = (FORMAT_NAME = 'my_json_fmt');\
\
SELECT * FROM sales_raw;\
\
CREATE OR REPLACE TABLE FLATTEN_DATA.SALES_FLATTEN AS\
SELECT\
  c.key                           AS COMPANIES,\
  c.value:sales_period     ::STRING AS SALES_PERIOD,\
  c.value:total_revenue    ::FLOAT  AS TOTAL_REVENUE,\
  c.value:total_units_sold ::FLOAT  AS TOTAL_UNITS_SOLD,\
  r.key                           AS REGIONS,\
  r.value:total_sales      ::FLOAT  AS TOTAL_SALES,\
  p.key                           AS PRODUCTS,\
  p.value:units_sold       ::FLOAT  AS UNITS_SOLD,\
  p.value:revenue          ::FLOAT  AS REVENUE\
FROM RAW_DATA.SALES_RAW  AS sr,\
  LATERAL FLATTEN(input => sr.DATA:"companies") AS c,\
  LATERAL FLATTEN(input => c.value:"regions")    AS r,\
  LATERAL FLATTEN(input => r.value:"products")   AS p;\
\
SELECT * FROM SALES_FLATTEN;\
\
-- Calculate the revenue for each company.\
SELECT COMPANIES,\
    SUM(REVENUE) AS TOTAL_REV\
FROM SALES_FLATTEN\
GROUP BY COMPANIES\
ORDER BY TOTAL_REV DESC;\
\
"""\
COMPANIES	TOTAL_REV\
Tech Supplies Inc.	1600000\
Gadget World Ltd.	1200000\
Tech Horizon	1100000\
SmartTech Enterprises	1000000\
Future Tech	800000\
Digital Innovations	750000\
ElectroWorld Inc.	750000\
TechMasters	750000\
Global Gadget Co.	600000\
"""\
-- Determine the top 3 companies with the highest revenue.\
SELECT COMPANIES,\
    SUM(REVENUE) AS TOTAL_REV\
FROM SALES_FLATTEN\
GROUP BY COMPANIES\
ORDER BY TOTAL_REV DESC\
LIMIT 3;\
\
"""\
COMPANIES	TOTAL_REV\
Tech Supplies Inc.	1600000\
Gadget World Ltd.	1200000\
Tech Horizon	1100000\
"""\
\
-- Identify the 2 regions with the lowest number of units sold.\
SELECT REGIONS,\
    SUM(UNITS_SOLD) AS TOTAL_UNITS_SOLD\
FROM SALES_FLATTEN\
GROUP BY REGIONS\
ORDER BY TOTAL_UNITS_SOLD\
LIMIT 2;\
\
"""\
REGIONS	TOTAL_UNITS_SOLD\
Asia	5000\
Europe	6200\
"""\
\
-- Identify the product with the highest revenue generated from sales.\
SELECT PRODUCTS,\
    SUM(REVENUE) AS TOTAL_REV\
FROM SALES_FLATTEN\
GROUP BY PRODUCTS\
ORDER BY TOTAL_REV DESC\
LIMIT 1;\
\
"""\
PRODUCTS	TOTAL_REV\
smartphone	3350000\
"""\
\
-- Find the product with the fewest units sold.\
SELECT PRODUCTS,\
    SUM(UNITS_SOLD) AS TOTAL_UNITS_SOLD\
FROM SALES_FLATTEN\
GROUP BY PRODUCTS\
ORDER BY TOTAL_UNITS_SOLD\
LIMIT 1;\
\
"""\
PRODUCTS	TOTAL_UNITS_SOLD\
laptop	9800\
"""\
\
-- Determine the region with the highest number of laptop units sold\
SELECT REGIONS,\
    SUM(UNITS_SOLD) AS TOTAL_LAPTOPS_SOLD\
FROM SALES_FLATTEN\
WHERE PRODUCTS = 'laptop'\
GROUP BY REGIONS\
ORDER BY TOTAL_LAPTOPS_SOLD DESC\
LIMIT 1;\
\
"""\
REGIONS	TOTAL_LAPTOPS_SOLD\
North America	5300\
"""\
\
-- Identify the bottom 3 companies and regions with the lowest revenue generated from smartphone sales.\
WITH CTE AS(\
    SELECT COMPANIES,\
        REGIONS,\
        PRODUCTS,\
        SUM(REVENUE) AS TOTAL_REV,\
        RANK() OVER(ORDER BY SUM(REVENUE)) AS RNK\
    FROM SALES_FLATTEN\
    WHERE PRODUCTS = 'smartphone'\
    GROUP BY  COMPANIES,\
        REGIONS,\
        PRODUCTS\
)\
SELECT COMPANIES,\
    REGIONS,\
    PRODUCTS,\
    TOTAL_REV\
FROM CTE\
WHERE RNK <= 3;\
\
"""\
COMPANIES	REGIONS	PRODUCTS	TOTAL_REV\
ElectroWorld Inc.	South America	smartphone	100000\
Future Tech	Asia	smartphone	100000\
SmartTech Enterprises	South America	smartphone	100000\
"""}