from typing import Container
from realapi import csv_generator
import pymongo
myclient = pymongo.MongoClient(
    "mongodb+srv://alf-deepen:GWvfeEk5w5TgcXSa@cluster0.ixkyxa7.mongodb.net/?retryWrites=true&w=majority")
mydb = myclient["papers"]
mycol = mydb["researchpapers"]

container = [
    << << << < HEAD
    ["patchclamp", 19705],
    ["neurodegenerative", 129444]
    == == == =
    ["brain imaging animal", 75055]
    >> >>>> > a299740193c144728a6082133f7dd28eedbc7d66
]

for item in container:

    mylist = csv_generator(item[0], item[1])
    duplicates = 0
    for document in mylist:
        try:
            insertid = mycol.insert_one(document)
        except:
            duplicates += 1

    print("there were ", str(duplicates), " Duplicates in the insertion")
