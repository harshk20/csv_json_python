# csv_json_python

Code challenges which can help understand/learn reading csv files, parsing json files,
establishing sql connections, making requests,auto generating files using python.

Pre-Requirements
- python (>3)

Packages used (planning to install them via setup.py in future)
- requests
- pandas
- pymysql
- json

Challenge1

This program takes sql server details as input
Reads csv data from https://footystats.org/
Sends data in chunks to pipedream API
Finally saves the data in provided sql database

How to run: execute football.py file with python
Input: SQL connection details (host, user, password, dbname)
Output:
Send json objects via post requests to pipedeam API in chunk of 20s
Save records in database tables using SQLs

Challenge2

This program takes JSON file path AND sql server details as input
Parses schema in json file
Generates multiple schema json files in output folder
Generates SQLs to create DB and tables using generated schema
Also generates SQLs to insert data for manual use (can further be automated)

How to run: execute main.py file with python
Input:
File path to shopify's order schema (.json)
SQL connection details (host, user, password, dbname)
Output:
Generating new table schemas after flattening the original json schema.
Generating SQL queries
Executing create database queries

Limitation:
Currently we need to hard code the alias names for similar object structures like billing_address, shipping_address, default_address all of them will have similar structures so lets just call them address and combine
Repeated strings/integers currently will be stored as comma separated plain text [This can be added to another table with primary key as string id and parent id (many to many relationship)]

Pending todos that can be implemented for improving this project:
1. Write unit tests
2. Write try catch sections
3. Write async calls if possible
4, Write store procedures for queries

