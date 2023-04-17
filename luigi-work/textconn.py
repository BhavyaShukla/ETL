import pymongo
import pandas as pd
from bson.json_util import dumps
import json
import csv
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["scs"]
mycol1 = mydb["case_age"]
mycol2 = mydb["case_death"]
mycol3 = mydb["case_country"]

mydoc1 = mycol1.find({})
mydoc2 = mycol2.find({});
mydoc3 = mycol3.find({});
# df1 =  pd.DataFrame(list(mydoc1))
# df2 =  pd.DataFrame(list(mydoc2))
# df3 =  pd.DataFrame(list(mydoc3))

# print("________________________________________")
# print(df1.head())
# print("________________________________________")
# print(df2.head())
# print("________________________________________")
# print(df3.head())
# print("________________________________________")
# f = self.output().open('w')
#         df1.to_csv(f,encoding = 'utf-8',index=False,header=True,quoting=2)
# f.close()
      
df1 = pd.read_csv("db1.csv", header = 0, encoding = 'utf-8',index_col = False)
df2 = pd.read_csv("db2.csv", header = 0, encoding = 'utf-8',index_col = False)
df3 = pd.read_csv("db3.csv", header = 0, encoding = 'utf-8',index_col = False)
# df4 = pd.merge(df1,df3,how='inner',on=['_id'])
# df5 = pd.merge(df3,df4,how='inner',on=['_id'])
frames = [df1, df3]
result = pd.concat(frames)

print("________________________________________")
print(df1.shape)
print("________________________________________")
print(df2.shape)
print("________________________________________")
print(df3.shape)
print("________________________________________")
print(result.shape)
print("________________________________________")

print("________________________________________")
print(df1.head())
print("________________________________________")
print(df2.head())
print("________________________________________")
print(df3.head())
print("________________________________________")



# for data in json_data:
#     if count == 0:
#         header = data.keys()
#         csv_writer.writerow(header)
#         count += 1
#     csv_writer.writerow(data.values())
 
# data_file.close()