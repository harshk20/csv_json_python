import common
import order
import clear

clear.cleanUp()

print("Enter file path to json file:")
path = input()
orderJSON = common.loadJsonFile(path)
if (orderJSON != None) :
    order.processOrder(orderJSON)
else:
    print("Invalid path : Re-run the program and try absolute file path")
