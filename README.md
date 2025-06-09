In this repository I have multiple data projects I did primarily in Snowflake with some AWS, DBT Cloud and Visual Studio as well. Projects include simiple data analysis to data engineering where I built full pipelines from AWS to Snowflake to DBT Cloud to Tableau.

Snowflake Project 1: 

Step 1: Load employee data from a CSV file into a raw table in Snowflake using the internal stage.
Step 2: Perform complex transformations on the raw data
Step 3: Load the transformed data into a new transformed table for further analysis.
Step 4: Data analysis

Architecture
CSV: Employee data -> uploaded -> Snowflake internal stage -> copy command -> raw table -> tansformed data -> transformed table -> data analysis

Snowflake Project 2:

Step 1: Load customer data from a JSON file into a raw table in Snowflake using the internal stage.
Step 2: Perform JSON flattening on the raw table and create a flatten table 
Step 3: Perform data analysis on the flatten data

JSON: Customer data -> uploaded -> Snowflake internal stage -> copy command -> raw table -> data flattening -> flattened table -> data analysis

