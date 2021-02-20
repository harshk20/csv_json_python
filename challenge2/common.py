## File contains common code to be used
import json
import os

## if file exists : load json into a python object and return the object
## Otherwise return None
def loadJsonFile (filePath):
    if os.path.exists(filePath) :
        with open(filePath, 'r') as json_data:
            loadedJSON = json.load(json_data)
            json_data.close()
            return loadedJSON
    return None

# Save json file and pretty text file 
def saveJsonFile(filePath, jsonObj):
    with open(filePath, 'w', encoding='utf-8') as f:
        json.dump(jsonObj, f, indent=4)
        f.close()
    
    with open(filePath[0:-5]+"_pretty.txt", 'w') as f_pretty:
        writeSchema(jsonObj, f_pretty)
        f_pretty.close()

# To check if the field is Non repeated no record field
def isSimpleField(field):
    if field['type'] != 'RECORD' and field['mode'] != "REPEATED":
        return True
    else:
        return False

# To get all the record fields of schema
def getRecordFields(schema, recordFields) :
    if 'schema' in schema:
        if 'fields' in schema['schema']:
            for f in schema['schema']['fields']:
                if f['type'] == 'RECORD':
                    recordFields.append(f['name'])

# To find a child field by name, if not found return none
def FindChildFieldByName(field, name):
    if 'fields' in field:
        for f in field['fields']:
            if f['name'].lower() == name.lower() :
                return f
    return None

# Fill all the fields in new parent
def FillFields(field, newParent):

    # See if we already have the child (if yes, then lets assume we have everything skip)
    if FindChildFieldByName(newParent, field['name']) == None :

        # If not found, then lets create a new field and append it to new parent
        newField = {}
        newField['type'] = field['type']
        newField['mode'] = field['mode']
        newField['name'] = field['name']

        # do the same for all the child fields
        if 'fields' in field:
            newFields = []
            for f in field['fields']:
                FillFields(f, newFields)
            newField['fields'] = newFields

        newParent.append(newField)

# To insert an id field in child/new schema
def insertIdField(srcSchema, newSchema) :

    idField = {}
    
    idField['name'] = (srcSchema['tableId'] + "_id") if 'tableId' in srcSchema else "parent_id"
    
    srcId = FindChildFieldByName(srcSchema['schema'], "id")
    idField['type'] = "INTEGER" if srcId == None else srcId["type"]
    idField['mode'] = "NULLABLE"

    # only if its not already present
    if 'fields' in newSchema['schema'] and \
        FindChildFieldByName(newSchema['schema'], idField['name']) == None :
            newSchema['schema']['fields'].append(idField)

# Find and remove field by name
def findAndRemove(field, sourceField) :
    
    rF = FindChildFieldByName(field, sourceField)
    if rF != None:
        field['fields'].remove(rF)
    if 'fields' in field:
        for f in field['fields']:
            findAndRemove(f, sourceField)

def RemoveSrcFields(srcSchema, sourceField):
    findAndRemove(srcSchema['schema'], sourceField)

# Find and extract the record fields and transfer them to new schema
def findAndExtract(field, newSchema, sourceField):

    ## Seee if this is the field we are looking for and it has fields under it
    if field['name'] == sourceField and \
        'fields' in field:
        
        ## Create new fields for new schema
        newFields = []

        ## Fill the new fields
        for f in field['fields']:
            FillFields(f, newFields)
        
        ## if there are already some fields present, then append the new fields
        ## Case when similar schema is present in multiple sub objects
        if 'fields' in newSchema['schema'] :
            for nf in newFields :
                if FindChildFieldByName(newSchema['schema'], nf['name']) == None :
                    newSchema['schema']['fields'].append(nf)
        ## If there are no fields then just add the new ones
        else :
            newSchema['schema']['fields'] = newFields

        ## We are done transferring the child fields, remove from source schema. 
        ## We will have to delete this field too
        field.pop('fields')

## To extract the records from src schema to new schema
def extractSchema (srcSchema, newSchema, sourceField):    
    if 'schema' in srcSchema and srcSchema['type'] == 'TABLE':
        if 'fields' in srcSchema['schema']:
            for field in srcSchema['schema']['fields']:
                findAndExtract(field, newSchema, sourceField)
        
            # Insert the Id field in child/new schema
            insertIdField(srcSchema, newSchema)
            # remove the src field, as we are done transferring
            RemoveSrcFields(srcSchema, sourceField)

###################################### To Print Schema Objects ####################################
## Print a field
def printField(field, depth, f):
    
    print("\t"*depth + field['name'],": ",field['type'], "*" if field['mode'] == "REPEATED" else "")
    
    if 'fields' in field:
        print("\t"*depth+"{")

        for f in field['fields']:
            printField(f, depth+1, f)

        print("\t"*depth+"}")
        
## Print the entire schema by walking fields
def printSchema(table, file = None):
    if 'schema' in table and table['type'] == 'TABLE':
        for field in table['schema']['fields']:
            printField(field, 0, file)



###################### To write the schema in pretty format to a file #############################

def writeField(field, depth, file):
    
    file.write("\t"*depth + field['name']+": "+field['type']+ ("*" if field['mode'] == "REPEATED" else "") + "\n")

    if 'fields' in field:
        file.write("\t"*depth+"{" + "\n")
        
        for f in field['fields']:
            writeField(f, depth+1, file)
        
        file.write("\t"*depth+"}" + "\n")
        

## Print the entire schema by walking fields
def writeSchema(table, file):
    if 'schema' in table and table['type'] == 'TABLE':
        file.write(table['tableId'] + "\n")

        if 'fields' in table['schema']:
            file.write("{" + "\n")

            for field in table['schema']['fields']:
                writeField(field, 1, file)
            
            file.write("}" + "\n")

