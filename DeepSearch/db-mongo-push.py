from realapi import csv_generator
import pymongo
myclient = pymongo.MongoClient("mongodb+srv://alf-deepen:GWvfeEk5w5TgcXSa@cluster0.ixkyxa7.mongodb.net/?retryWrites=true&w=majority")
mydb = myclient["papers"]
mycol = mydb["researchpapers"]

mylist = csv_generator('brain implant injury',50)
duplicates = 0
for document in mylist:
    try:
        insertid = mycol.insert_many(document)
    except:
        duplicates += 1

print("there were ", str(duplicates), " Duplicates in the insertion")
