import common
import order
import clear
import generate_sql
import pymysql as mysqlcon
import getpass

# Get rid of old generated data in output folder
clear.cleanUp()

print ("\nThis program takes JSON file path AND sql server details as input" + \
      "\nParses schema in json file"+ \
      "\nGenerates multiple schema json files in output folder"+ \
      "\nGenerates SQLs to create DB and tables using generated schema"+ \
      "\nAlso generates SQLs to insert data for manual use (can further be automated)")

print("\nProgram has started.")

# Get path to json file
path = input("Enter file path to shopify orders json file:")

# Load the file
orderJSON = common.loadJsonFile(path)
if (orderJSON != None) :

    # Process the order (break down in multiple tables)
    order.processOrder(orderJSON)
    print("Order schema processing is done.")

    # Get sql details
    print(" ---------- SQL Connection Details -----------")

    host = input("host: ") 
    user = input("user: ")
    password = getpass.getpass("password :") 
    db = input("Database name: ")

    ## SQLs Create the database using name given in input
    createDBSQL = "CREATE DATABASE IF NOT EXISTS " + db + ";\n"
    createDBSQL += "USE " + db + ";\n"
    generate_sql.writeToSQLFile("sql_create_table", createDBSQL)

    ## Generate SQLs to create the tables
    all_tables = []
    alterForeignKeys = []
    generate_sql.generateSQLs(orderJSON, "order", None, all_tables, all_tables, alterForeignKeys)

    ## Generate Insert Into sqls (Using same function with a flag for now.)
    ## Todo: Separate it out completely, (generate using create table sqls)
    generated_tables = []
    generate_sql.generateSQLs(orderJSON, "order", None, generated_tables, all_tables, alterForeignKeys, True)
    print("SQLs generated.")

    ## connect to sql server
    engine = mysqlcon.Connect(host=host, user = user, password = password, cursorclass = mysqlcon.cursors.DictCursor)
    
    ## Get the cursor
    with engine:
        with engine.cursor() as cursor :

            ## Get the create db sql
            getCreateDBSQL = generate_sql.readSQLFile("sql_create_table")
            insertSQL = generate_sql.readSQLFile("insert_into") ## This can be used for persisting data
    
            if getCreateDBSQL != None:
                queries = getCreateDBSQL.split(';')
                for q in queries:
                    if q != "" and q != " " and q != "\n" :
                        ## lets create tables, one after the other
                        cursor.execute(q)
                engine.commit()
else:
    print("Invalid path : Re-run the program and try absolute file path")

print("Program has ended.")