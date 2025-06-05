{\rtf1\ansi\ansicpg1252\cocoartf2580
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000;\
\
SELECT\
    c.C_CUSTKEY,\
    c.C_NAME,\
    COUNT(DISTINCT o.O_ORDERKEY) as num_orders\
FROM\
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.customer c\
JOIN\
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.orders o\
ON\
    c.C_CUSTKEY = o.O_CUSTKEY\
GROUP BY\
    c.C_CUSTKEY, c.C_NAME\
ORDER BY\
    num_orders DESC\
LIMIT  5;\
\
\
SELECT\
  C_CUSTKEY AS CUSTOMER_KEY,\
  C_NAME AS CUSTOMER_NAME,\
  num_orders as NUMBER_OF_ORDERS\
FROM\
  (\
    SELECT\
      c.C_CUSTKEY,\
      c.C_NAME,\
      COUNT(o.O_ORDERKEY) as num_orders,\
      ROW_NUMBER() OVER (ORDER BY COUNT(o.O_ORDERKEY) DESC) as row_num\
    FROM\
      SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.customer c\
    JOIN\
      SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.orders o\
    ON\
      c.C_CUSTKEY = o.O_CUSTKEY\
    GROUP BY\
      c.C_CUSTKEY, c.C_NAME\
  ) subquery\
WHERE\
  row_num <= 5;\
\
SELECT\
  n.N_NATIONKEY,\
  n.N_NAME AS nation,\
  AVG(l.L_QUANTITY) AS avg_quantity_shipped\
FROM\
  SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.nation n\
JOIN\
  supplier s ON n.N_NATIONKEY = s.S_NATIONKEY\
JOIN\
  lineitem l ON s.S_SUPPKEY = l.L_SUPPKEY\
GROUP BY\
  n.N_NATIONKEY, n.N_NAME\
ORDER BY\
  avg_quantity_shipped DESC\
LIMIT\
  5;\
\
SELECT\
  (CASE WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 1 THEN 'JANUARY'\
   WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 2 THEN 'FEBRUARY'\
   WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 3 THEN 'MARCH'\
   WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 4 THEN 'APRIL'\
   WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 5 THEN 'MAY'\
   WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 6 THEN 'JUNE'\
   WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 7 THEN 'JULY'\
   WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 8 THEN 'AUGUST'\
   WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 9 THEN 'SEPTEMBER'\
   WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 10 THEN 'OCTOBER'\
   WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 11 THEN 'NOVEMBER'\
   WHEN EXTRACT(MONTH FROM o.O_ORDERDATE) = 12 THEN 'DECEMBER'\
   END) as order_month,\
  COUNT(DISTINCT o.O_ORDERKEY) as num_orders,\
  SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT)) as total_revenue\
FROM\
  snowflake_sample_data.tpch_sf1000.orders o\
JOIN\
  snowflake_sample_data.tpch_sf1000.lineitem l\
ON\
  o.O_ORDERKEY = l.L_ORDERKEY\
WHERE\
  EXTRACT(YEAR FROM o.O_ORDERDATE) = '1997'\
GROUP BY\
  order_month\
ORDER BY\
  order_month;}