import pymongo
import csv

myclient = pymongo.MongoClient("mongodb+srv://alf-deepen:GWvfeEk5w5TgcXSa@cluster0.ixkyxa7.mongodb.net/?retryWrites=true&w=majority")
mydb = myclient["papers"]
mycol = mydb["researchpapers"]

path = 'multiphoton_data_4000.csv'
input_file = csv.DictReader(open(path))

duplicates = 0
counter = 0

for row in input_file:
    row.pop('')
    try:
        insertid = mycol.insert_one(row)
    except:
        duplicates += 1
    counter +=1
    if counter%100==0:
        print(counter, 'current state AND duplicates: ',duplicates)
