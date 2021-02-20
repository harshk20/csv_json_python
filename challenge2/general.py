import common

# save the schema objec in json format using the given name
def saveGenericObj (name, schema):
    common.saveJsonFile('challenge2/output/'+ name+'.json', schema)

# create schema object from file if present (to append) Otherwise create new
def CreateGenericSchema(name):
    generic = common.loadJsonFile('challenge2/output/'+name+'.json')
    if generic != None and 'schema' in generic :
        return generic

    generic = {
                'type' : "TABLE",
                'tableId' : name,
                'schema': {
                }
            }
    return generic

# Recursively process all the record objects 
def processGenericObj (schema) :
    
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
    
    # get all record fields
    recordFields = []
    common.getRecordFields(schema, recordFields)
    ## if there are none, then lets go back
    if recordFields.count == 0:
        return

    # otherwise lets walk through all record fields and generate new schemas
    for rf in recordFields :
        ## use alias name if present to create schema
        name = rf
        if rf in alias:
            name = alias[rf]
        newSchema = CreateGenericSchema(name)
        # extract schema from source to new schema 
        common.extractSchema(schema, newSchema, rf)
        processGenericObj(newSchema)
        # save schema
        saveGenericObj(name, newSchema)
    return
