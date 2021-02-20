import json
import common
import general

# to save order object
def saveOrder (orderObj):
    common.saveJsonFile('challenge2/output/order.json', orderObj)

# to create order object
def CreateNewOrderSchema():

    order = common.loadJsonFile('challenge2/output/order.json')
    if order != None and 'schema' in order :
        return order

    order = {
                'type' : "TABLE",
                'tableId' : "Order",
                'schema': {
                }
            }
    return order

## NormalizeField meaning we are going to desolve this field 
## and bring out the child fields in the parent after changing their names
def NormalizeField(field, newField):

    if field['type'] != "RECORD" or \
       field['mode'] == "REPEATED" or \
        'address' in field['name'] or \
        'location' in field['name']:
        return False

    if 'fields' in field:
        for f in field['fields']:
            if common.isSimpleField(f) == False :
                return False

    for f in field['fields']:
        nf = {}
        nf['name'] = field['name'] + '.' + f['name']
        nf['type'] = f['type']
        nf['mode'] = f['mode']
        newField.append(nf)

    return True
    
def parseField(field, newParent):
    
        newField = {}
        newField['type'] = field['type']
        newField['mode'] = field['mode']
        newField['name'] = field['name']

        if 'fields' in field:
            newFields = []
            for f in field['fields']:
                parseField(f, newFields)
            newField['fields'] = newFields
        
        # if the field is normalized then its already broken into multiple subfields
        # and added to new parent
        if NormalizeField(newField, newParent) == False :
            newParent.append(newField)

# To normalize/flatten non repeated simple records
def parseOrder(order, newOrder):
    if 'schema' in order and order['type'] == 'TABLE':
        if 'fields' in order['schema']:
            newFields = []
            for field in order['schema']['fields']:
                parseField(field, newFields)
            newOrder['schema']['fields'] = newFields

def processOrder(orderSchema) :
    # Get new order schema
    newOrderJSON = CreateNewOrderSchema()
    parseOrder (orderSchema, newOrderJSON)
    general.processGenericObj(newOrderJSON)
    # save order
    saveOrder(newOrderJSON)