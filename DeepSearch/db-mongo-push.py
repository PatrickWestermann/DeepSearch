from realapi import csv_generator
import pymongo
myclient = pymongo.MongoClient("mongodb+srv://alf-deepen:GWvfeEk5w5TgcXSa@cluster0.ixkyxa7.mongodb.net/?retryWrites=true&w=majority")
mydb = myclient["papers"]
mycol = mydb["researchpapers"]

mylist = csv_generator('mouse brain damage',50)
insertid = mycol.insert_many(mylist)
