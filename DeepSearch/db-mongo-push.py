from typing import Container
from realapi import csv_generator
import pymongo
myclient = pymongo.MongoClient("mongodb+srv://alf-deepen:GWvfeEk5w5TgcXSa@cluster0.ixkyxa7.mongodb.net/?retryWrites=true&w=majority")
mydb = myclient["papers"]
mycol = mydb["researchpapers"]

container = [
    ["neuronal circuit",36695],
    ["voltage imaging",13362],
    ["brain imaging animal",75055]
]

for item in container:

    mylist = csv_generator(item[0],item[1])
    duplicates = 0
    for document in mylist:
        try:
            insertid = mycol.insert_many(document)
        except:
            duplicates += 1

    print("there were ", str(duplicates), " Duplicates in the insertion")
