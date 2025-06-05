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
CREATE OR REPLACE DATABASE EMPLOYEE_DATA;\
\
CREATE OR REPLACE SCHEMA RAW_DATA;\
\
CREATE OR REPLACE SCHEMA TRANSFORMED_DATA;\
\
USE DATABASE EMPLOYEE_DATA;\
\
USE SCHEMA RAW_DATA;\
\
CREATE OR REPLACE TABLE EMPLOYEE_RAW\
(\
EMPLOYEE_ID STRING,\
FIRST_NAME STRING,\
LAST_NAME STRING,\
DEPARTMENT STRING,\
SALARY DECIMAL(10,2),\
HIRE_DATE DATE,\
LOCATION STRING\
)\
\
USE SCHEMA TRANSFORMED_DATA;\
\
CREATE OR REPLACE TABLE TRANSFORMED_DATA\
(\
EMPLOYEE_ID STRING,\
FULL_NAME STRING,\
DEPARTMENT STRING,\
ANNUAL_SALARY DECIMAL(10, 2),\
HIRE_DATE DATE,\
EXPERIENCE_LEVEL STRING,\
TENURE_DAYS STRING,\
STATE STRING,\
COUNTRY STRING,\
BONUS_ELIGIBILITY STRING,\
HIGH_POTENTIAL_FLAG STRING\
);\
\
ALTER TABLE EMPLOYEE_RAW SET DATA_RETENTION_TIME_IN_DAYS = 90;\
\
ALTER TABLE TRANSFORMED_DATA SET DATA_RETENTION_TIME_IN_DAYS = 90;\
\
CREATE OR REPLACE STAGE EMPLOYEE_STAGE;\
\
LIST @EMPLOYEE_STAGE;\
\
COPY INTO EMPLOYEE_RAW\
FROM @EMPLOYEE_STAGE\
FILE_FORMAT =(TYPE ='CSV' SKIP_HEADER=1)\
\
SELECT * FROM EMPLOYEE_RAW;\
\
-- Full Name: Concatenate first_name and last_name.\
SELECT employee_id,\
    CONCAT(first_name, ' ', last_name) AS full_name\
FROM EMPLOYEE_RAW;\
\
-- yearly salary\
SELECT employee_id,\
    salary * 12 AS annual_salary\
FROM EMPLOYEE_RAW;\
\
-- Experience Level: Classify employees based on the hire date. For example:\
-- New Hire: Less than 1 year.\
-- Mid-level: 1 to 5 years.\
-- Senior: More than 5 years.\
SELECT employee_id, \
    hire_date,\
    CASE \
        WHEN DATEDIFF(day, HIRE_DATE, CURRENT_DATE())/365 < 1 THEN 'New Hire'\
        WHEN DATEDIFF(day, HIRE_DATE, CURRENT_DATE())/365 BETWEEN 1 AND 5 THEN 'Mid_Level'\
        ELSE 'Senior' END AS EXPERIENCE_LEVEL\
FROM EMPLOYEE_RAW;\
\
-- Employee Tenure: Calculate how long an employee has been with the company based on the hire_date in days\
SELECT employee_id,\
    DATEDIFF(day, HIRE_DATE, '2024-12-31') AS TENURE_DAYS\
FROM EMPLOYEE_RAW;\
\
-- State: Fetch the value before the hyphen(-) in the location column\
SELECT employee_id,\
    SUBSTR(location, 1, POSITION('-' IN location) - 1) AS city\
FROM EMPLOYEE_RAW;\
\
-- Country: Fetch the value after the hyphen(-) in the location column\
SELECT employee_id,\
  SUBSTR(location, POSITION('-' IN location) + 1) AS country\
FROM EMPLOYEE_RAW;\
\
-- Employee's Eligibility for Bonus: For example, employees with a salary greater than $ 10,000 are eligible for a bonus.\
SELECT employee_id,\
    salary,\
    CASE WHEN salary > 10000 THEN 'Bonus Eligible' ELSE 'No Bonus' END \
FROM EMPLOYEE_RAW;\
\
-- Flagging High-Potential Employees: Flag employees who have been with the company for more than 3 years.\
SELECT employee_id,\
    DATEDIFF(day, HIRE_DATE, CURRENT_DATE()) AS TENURE_DAYS,\
    CASE WHEN DATEDIFF(day, HIRE_DATE, CURRENT_DATE())/365 > 3 THEN 'High Potential'        ELSE 'Under 3 Years' END\
FROM EMPLOYEE_RAW;\
\
-- above aggregations put in one query\
CREATE OR REPLACE TABLE TRANSFORMED_DATA.transformed_data AS\
WITH cte AS(    \
    SELECT\
        employee_id,\
        first_name,\
        last_name,\
        department,\
        salary,\
        hire_date,\
        location,\
        DATEDIFF(day, HIRE_DATE, '2024-12-31') AS tenure_days,\
        DATEDIFF(day, hire_date, CURRENT_DATE())/365.0 AS years_of_service\
    FROM EMPLOYEE_RAW\
)\
SELECT \
    employee_id,\
    CONCAT(first_name, ' ', last_name) AS full_name,\
    department,\
    salary * 12 AS annual_salary,\
    hire_date,\
    CASE\
        WHEN years_of_service < 1 THEN 'New Hire'\
        WHEN years_of_service BETWEEN 1 AND 5 THEN 'Mid_Level'\
        ELSE 'Senior'\
    END AS experience_level,\
    tenure_days,\
    SUBSTR(location, 1, POSITION('-' IN location) - 1) AS state,\
    SUBSTR(location, POSITION('-' IN location) + 1) AS country,\
    CASE \
        WHEN salary > 10000 THEN 'Bonus Eligible' \
        ELSE 'No Bonus' \
    END AS bonus_eligibility,  \
    CASE \
        WHEN years_of_service > 3 THEN 'High Potential'        \
        ELSE 'Under 3 Years' \
    END AS high_potential_flag\
FROM cte;\
\
USE SCHEMA TRANSFORMED_DATA;\
\
SELECT * FROM transformed_data LIMIT 5;\
"""\
EMPLOYEE_ID	FULL_NAME	DEPARTMENT	ANNUAL_SALARY	HIRE_DATE	EXPERIENCE_LEVEL	TENURE_DAYS	STATE	COUNTRY	BONUS_ELIGIBILITY	HIGH_POTENTIAL_FLAG\
E001	John Doe	Engineering	60000.00	2024-03-15	Mid_Level	291	New York	USA	No Bonus	Under 3 Years\
E002	Jane Smith	Marketing	48000.00	2021-06-22	Mid_Level	1288	England	United Kingdom	No Bonus	High Potential\
E003	Jim Brown	Sales	36000.00	2024-08-30	New Hire	123	Paris	France	No Bonus	Under 3 Years\
E004	Linda White	HR	42000.00	2020-01-10	Senior	1817	Berlin	Germany	No Bonus	High Potential\
E005	Michael Johnson	Engineering	72000.00	2018-11-12	Senior	2241	Tokyo	Japan	No Bonus	High Potential\
"""\
-- Employee Count by Department\
SELECT \
    department,\
    COUNT(employee_id) AS employee_count_department\
FROM transformed_data\
GROUP BY department;\
"""\
DEPARTMENT	EMPLOYEE_COUNT_DEPARTMENT\
Engineering	25\
Marketing	25\
Sales	25\
HR	25\
"""\
-- Provide count of employees by country\
SELECT \
    country,\
    COUNT(employee_id) AS employee_count_country\
FROM transformed_data\
GROUP BY country\
ORDER BY employee_count_country DESC;\
"""\
COUNTRY	EMPLOYEE_COUNT_COUNTRY\
Australia	10\
United Kingdom	8\
France	8\
Japan	8\
Argentina	6\
Canada	5\
Brazil	4\
India	4\
South Korea	4\
USA	4\
Spain	3\
Venezuela	3\
Malaysia	3\
Sweden	3\
South Africa	3\
El Salvador	2\
Nigeria	2\
Colombia	2\
Mexico	2\
Netherlands	2\
Germany	2\
Ireland	1\
Belgium	1\
Costa Rica	1\
Chile	1\
Peru	1\
Rwanda	1\
Kenya	1\
China	1\
Finland	1\
Italy	1\
Norway	1\
Indonesia	1\
"""\
-- Extract employees who were hired within 12 months\
SELECT\
    employee_id,\
    full_name,\
    hire_date,\
    tenure_days\
FROM transformed_data\
WHERE tenure_days < 365;\
"""\
EMPLOYEE_ID	FULL_NAME	HIRE_DATE	TENURE_DAYS\
E001	John Doe	2024-03-15	291\
E003	Jim Brown	2024-08-30	123\
E012	Patricia Garcia	2024-11-30	31\
E027	Jessica Gonzalez	2024-02-04	331\
E047	David White	2024-04-30	245\
E051	William Williams	2024-09-14	108\
E052	Susan Green	2024-03-30	276\
E064	Robert Thomas	2024-08-24	129\
E066	Mary Roberts	2024-02-09	326\
E090	Joshua Baker	2024-07-13	171\
"""\
-- Extract the top 10% of employees by salary\
WITH cte AS(\
    SELECT \
        employee_id,\
        full_name,\
        annual_salary,\
        RANK() OVER(ORDER BY annual_salary DESC) AS rnk,\
        0.1*(SELECT COUNT(employee_id) FROM transformed_data) AS top_10percent_count\
    FROM transformed_data\
)\
SELECT \
    employee_id,\
    full_name,\
    annual_salary,\
    rnk\
FROM cte\
WHERE rnk <= top_10percent_count; \
"""\
EMPLOYEE_ID	FULL_NAME	ANNUAL_SALARY	RNK\
E013	Robert Martinez	720000.00	1\
E099	John Hall	564000.00	2\
E048	Linda Jackson	480000.00	3\
E027	Jessica Gonzalez	420000.00	4\
E082	James Baker	396000.00	5\
E042	Susan Hall	324000.00	6\
E016	Lisa Allen	300000.00	7\
E087	Jessica Thomas	252000.00	8\
E034	Karen Carter	240000.00	9\
E056	Nancy Walker	192000.00	10\
"""\
-- Determine how many employees with 5+ years with company\
WITH cte AS(\
    SELECT \
        employee_id,\
        full_name,\
        hire_date,\
        tenure_days,\
        experience_level\
    FROM transformed_data\
    WHERE experience_level = 'Senior'\
)\
SELECT COUNT(*) AS senior_employee_count\
FROM cte;\
"""\
SENIOR_EMPLOYEE_COUNT\
15\
"""\
SELECT \
    department,\
    YEAR(hire_date) AS year,\
    SUM(annual_salary)\
FROM \
    transformed_data\
WHERE\
    year = 2018\
GROUP BY\
    department, year;\
"""\
DEPARTMENT	YEAR	SUM(ANNUAL_SALARY)\
Engineering	2018	211200.00\
Sales	2018	39600.00\
"""\
-- Calculate the total salary expense per department for each year. (2018-2024 yearend)\
-- I read this question different than DEA did. Their answer is below but based on how the question was asked I think my answer is more accurate, they only count salaries from NEW HIRES. The way the question should be asked is what is the Salary Expense per Department per Year for new hires. But even this doesn't properly answer the quesiton as we know the hire date of each employee, they didn't work the entire year and this needs thier annual salary should be prorated by how many days they worked in that year. My answer has prorated salaries for each employees first year based off of the hire date and I assumed nobody was fired so I added their salaries for every year after thier first year into the total Salary Expense\
WITH cte AS(\
    SELECT\
        employee_id,\
        department,\
        annual_salary,\
        hire_date,\
        YEAR(hire_date) AS hire_year,\
        tenure_days,\
        tenure_days/365 AS tenure_years,\
        FLOOR(tenure_days/365) AS complete_years_worked,\
        tenure_days/365 - FLOOR(tenure_days/365) AS first_year_factor,\
        ROUND((tenure_days/365 - FLOOR(tenure_days/365)) * annual_salary, 2) AS first_year_salary\
    FROM transformed_data\
)\
SELECT department,\
    SUM(CASE WHEN hire_year = 2018 THEN first_year_salary ELSE 0 END) AS salary_cost_2018,\
    SUM(CASE WHEN hire_year = 2019 THEN first_year_salary ELSE 0 END) +\
    SUM(CASE WHEN hire_year < 2019 THEN annual_salary ELSE 0 END) AS salary_cost_2019,\
    SUM(CASE WHEN hire_year = 2020 THEN first_year_salary ELSE 0 END) +\
    SUM(CASE WHEN hire_year < 2020 THEN annual_salary ELSE 0 END) AS salary_cost_2020,\
    SUM(CASE WHEN hire_year = 2021 THEN first_year_salary ELSE 0 END) +\
    SUM(CASE WHEN hire_year < 2021 THEN annual_salary ELSE 0 END) AS salary_cost_2021,\
    SUM(CASE WHEN hire_year = 2022 THEN first_year_salary ELSE 0 END) +\
    SUM(CASE WHEN hire_year < 2022 THEN annual_salary ELSE 0 END) AS salary_cost_2022,\
    SUM(CASE WHEN hire_year = 2023 THEN first_year_salary ELSE 0 END) +\
    SUM(CASE WHEN hire_year < 2023 THEN annual_salary ELSE 0 END) AS salary_cost_2023,\
    SUM(CASE WHEN hire_year = 2024 THEN first_year_salary ELSE 0 END) +\
    SUM(CASE WHEN hire_year < 2024 THEN annual_salary ELSE 0 END) AS salary_cost_2024  \
FROM cte\
GROUP BY department;\
"""\
DEPARTMENT	SALARY_COST_2018	SALARY_COST_2019	SALARY_COST_2020	SALARY_COST_2021	SALARY_COST_2022	SALARY_COST_2023	SALARY_COST_2024\
Engineering	76734.23	211200.00	670563.44	1858241.11	2376069.01	2434800.00	2482635.60\
Marketing	0.00	39767.67	499236.34	999751.19	1865947.41	2102163.26	2221453.16\
Sales	5316.18	39600.00	199709.56	417374.82	848940.88	1381060.28	2081061.22\
HR	0.00	0.00	340343.38	1018701.36	1242052.55	1743419.16	1926821.94\
"""\
-- this is how DEA answered the above question, I also did this query below but again the way the question is structured this is incorrect in my opinion.\
SELECT\
DISTINCT\
DEPARTMENT AS DEPARTMENT,\
EXTRACT(YEAR,HIRE_DATE) AS YEAR,\
SUM(ANNUAL_SALARY)\
FROM transformed_data\
GROUP BY ALL\
ORDER BY DEPARTMENT ASC,YEAR DESC;\
"""\
DEPARTMENT	YEAR	SUM(ANNUAL_SALARY)\
Engineering	2024	60000.00\
Engineering	2022	69600.00\
Engineering	2021	967200.00\
Engineering	2020	1186800.00\
Engineering	2018	211200.00\
HR	2024	138000.00\
HR	2023	493200.00\
HR	2022	238800.00\
HR	2021	195600.00\
HR	2020	943200.00\
Marketing	2024	110400.00\
Marketing	2023	170400.00\
Marketing	2022	277200.00\
Marketing	2021	1128000.00\
Marketing	2020	513600.00\
Marketing	2019	57600.00\
Sales	2024	536400.00\
Sales	2023	294000.00\
Sales	2022	760800.00\
Sales	2021	316200.00\
Sales	2020	238800.00\
Sales	2018	39600.00\
"""\
-- basic calculation for salary cost per department per year not the correct answer in my opinion\
SELECT department,\
    YEAR(hire_date) AS year,\
    SUM(annual_salary)\
FROM transformed_data\
GROUP BY department,\
    year\
ORDER BY department,\
    year DESC;\
\
-- used to test results of large case statement\
SELECT\
    department,\
    AVG(annual_salary),\
    COUNT(employee_id) AS employee_count\
FROM \
    transformed_data\
WHERE \
    YEAR(hire_date) <= 2024\
GROUP BY\
     department;\
\
-- used to test results of large case statement\
SELECT\
    employee_id,\
    department,\
    annual_salary,\
    hire_date,\
    YEAR(hire_date) AS hire_year,\
    tenure_days,\
    tenure_days/365 AS tenure_years,\
    FLOOR(tenure_days/365) AS complete_years_worked,\
    tenure_days/365 - FLOOR(tenure_days/365) AS first_year_factor,\
    ROUND((tenure_days/365 - FLOOR(tenure_days/365)) * annual_salary, 2) AS first_year_salary\
FROM transformed_data\
WHERE department = 'Engineering'\
\
\
\
}