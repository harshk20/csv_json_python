# csv_json_python

Pre-Requirements
- python (>3)

Packages used (planning to install them via setup.py in future)
- requests
- pandas
- sqlalchemy
- json

Challenge1

Run football.py file with python


Input:
SQL connection in format (user:password@sqldb:port/dbname)

Output:
Post requests sent to pipedeam url
Saving players and teams in database

Challenge2

Run main.py file with python


Input:
File path to shopify's order schema (.json)

Output:
Multiple json files in output folder.
Json files are schema of proposed new tables after flattening the original json.

Limitation:
Currently we need to hard code the alias names for similar object structures like billing_address, shipping_address, default_address all of them will have similar structures so lets just call them address and combine
