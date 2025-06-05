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
CREATE OR REPLACE DATABASE TIMETRAVEL_DB;\
\
CREATE OR REPLACE SCHEMA TIMETRAVEL_DATA;\
\
CREATE OR REPLACE TABLE EMPLOYEE\
(\
EMPLOYEE_ID STRING,\
FIRST_NAME STRING,\
LAST_NAME STRING,\
DEPARTMENT STRING,\
SALARY FLOAT,\
HIRE_DATE DATE\
);\
\
INSERT INTO TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE VALUES \
\
('E1', 'John', 'Doe', 'Finance', 75000.50, '2020-01-15'), \
\
('E2', 'Jane', 'Smith', 'HR', 68000.00, '2018-03-20'), \
\
('E3', 'Alice', 'Johnson', 'IT', 92000.75, '2019-07-10'),\
\
 ('E4', 'Bob', 'Williams', 'Sales', 58000.25, '2021-06-01'), \
\
('E5', 'Charlie', 'Brown', 'Marketing', 72000.00, '2022-04-22'), \
\
('E6', 'Emily', 'Davis', 'IT', 89000.10, '2017-11-12'), \
\
('E7', 'Frank', 'Miller', 'Finance', 83000.30, '2016-09-05'), \
\
('E8', 'Grace', 'Taylor', 'Sales', 61000.45, '2023-02-11'),\
\
 ('E9', 'Hannah', 'Moore', 'HR', 67000.80, '2020-05-18'), \
\
('E10', 'Jack', 'White', 'Marketing', 70000.90, '2019-12-25');\
\
SELECT * FROM EMPLOYEE;\
\
DELETE\
FROM EMPLOYEE\
WHERE EMPLOYEE_ID = 'E2';\
\
DELETE\
FROM EMPLOYEE\
WHERE EMPLOYEE_ID = 'E7';\
\
SELECT * FROM EMPLOYEE;\
-- Both E2 and E7 have been deleted\
\
SELECT * FROM EMPLOYEE BEFORE(STATEMENT => '01bc0f6d-0105-1f8d-0007-f65300035006');\
-- Shows table with E2 and E7\
\
CREATE OR REPLACE TABLE TIMETRAVEL_DB.TIMETRAVEL_DATA.EMPLOYEE AS\
SELECT * FROM EMPLOYEE BEFORE(STATEMENT => '01bc0f6d-0105-1f8d-0007-f65300035006');\
\
SELECT * FROM EMPLOYEE;\
-- All records have been recovered\
}