import common
import os

def writeToSQLFile(name, buf) :
    with open("challenge2/output/" + name + ".sql", 'a') as f:
        f.write(buf)
        f.close()

def readSQLFile(name) :
    if os.path.exists("challenge2/output/" + name + ".sql") :
        with open("challenge2/output/" + name + ".sql", "r") as f:
            output = f.read()
            f.close()
            return output
    return None

## Function to generate sqls
## allTables is just to support foreign keys insert via select query, we need all tables to identify foreign keys
def generateSQLs(schema, tableName, parentTable, generatedTables, allTables, alterForeignKeys,insert_sql = False) :

    ## Get the table json
    generic = common.loadJsonFile('challenge2/output/'+tableName+'.json')
    if generic != None :
        if 'schema' in generic:
            actualGenSchema = generic['schema']
        
        if 'fields' not in actualGenSchema:
            return

        ## Meaning we have already created it, so lets alter, add missing foreign keys and return
        if (tableName in generatedTables and parentTable != None):
            ## assuming the last table added 
            f = common.FindChildFieldByName(actualGenSchema, parentTable + "_id")
            if f != None:
                alterTableSql = "ALTER TABLE `" + tableName + "` \n"
                alterTableSql += "ADD FOREIGN KEY ( `" + f['name'] + "` ) REFERENCES `" + f['name'][0:-3] + "` ( `id` )\n"
                alterTableSql += "ON DELETE CASCADE;\n"
                if parentTable not in alterForeignKeys:
                    alterForeignKeys.append(parentTable)
                    writeToSQLFile("sql_create_table", alterTableSql)
            return

        ## Otherwise lets create table
        createTableSql = "CREATE TABLE IF NOT EXISTS `" + tableName + "` ( \n"
        insertIntoSql = 'INSERT INTO `' + tableName + '` ('
        valuesSql = 'VALUES ('

        ## foreignKeys so far
        foreignKeys = []
        for field in actualGenSchema['fields'] :
            
            ## capture foriegn keys
            if field['name'][0:-3] in generatedTables:
                foreignKeys.append(field['name'])
            
            if field['name'][0:-3] in allTables:
                valuesSql += '(SELECT `id` FROM `' + field['name'][0:-3] +\
                             '` WHERE `id` = {' + field['name'] + '}),'
            
            elif field['type'] == "STRING" :
                valuesSql += '"{' + field['name'] + '}",'
            else:
                valuesSql += '{' + field['name'] + '},'

            createTableSql += " `" + field['name'] + "` "
            insertIntoSql += " `" + field['name'] + "` ,"

            if (field['type'] == "NUMERIC" or \
                field['type'] == "INTEGER" or \
                field['type'] == "FLOAT" or \
                field['type'] == "BOOLEAN" or \
                field['type'] == "TIMESTAMP") :
                createTableSql += field['type'] + ", \n"

            elif field['type'] == "RECORD":
                print("This was unexpected, should assert here...")
            ## This includes string as well
            else:
                createTableSql += "VARCHAR(255), \n"

        pf = common.FindChildFieldByName(generic['schema'], "id")
        if pf != None:
            createTableSql += " PRIMARY KEY ( `" + pf['name'] + "` )"
        else :
            createTableSql += " `id` INTEGER NOT NULL AUTO_INCREMENT, \n"
            #insertIntoSql += " `id` ,"
            #valuesSql += ' {id} ,'
            createTableSql += " PRIMARY KEY ( `id` )"

        for f in foreignKeys:
            createTableSql += ", FOREIGN KEY ( `" + f + "` ) REFERENCES `" + f[0:-3] + "` ( `id` )\n"
            createTableSql += " ON DELETE CASCADE \n"
        
        insertIntoSql = insertIntoSql[0:-1]
        valuesSql = valuesSql[0:-1]

        insertSql = insertIntoSql + ")\n" + valuesSql + ");\n"

        if pf != None:
            createTableSql += " ) ENGINE=InnoDB;\n"
        else:
             createTableSql += " ) ENGINE=InnoDB AUTO_INCREMENT=1;\n"
        if insert_sql:
            generatedTables.append(tableName)
            writeToSQLFile("insert_into", insertSql)
        else:
            generatedTables.append(tableName)
            writeToSQLFile("sql_create_table", createTableSql)


    if 'schema' in schema :
        actualSchema = schema['schema']
    elif 'fields' in  schema:
        actualSchema = schema
        
    if 'fields' in actualSchema:
        ## alias to be used for similar type of objects
        alias = {
            "line_items" : "order_item",
            "line_item" : "order_item",
            "default_address" : "address",
            "origin_location" : "address",
            "note_attributes" : "properties",
            "destination_location" : "address",
            "billing_address" : "address",
            "shipping_address" : "address"
        }
    
        for f in actualSchema['fields']:
            if f['type'] == 'RECORD':
                ## use alias name if present to create schema
                name = f['name']
                if name in alias:
                    name = alias[name]
                    
                # recursively generate sqls for sub objects to maintain the order in which
                # these sqls can be executed
                generateSQLs(f, name, tableName, generatedTables, allTables, alterForeignKeys,insert_sql)
    return