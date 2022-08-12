from operator import index
import os
import pandas as pd
import csv
import glob

path = os.path.dirname(__file__)


updatedpath= path + '/User-Tweets'


UserIDList=os.listdir(updatedpath)

print(len(UserIDList))

for i in range(len(UserIDList)):
    UserIDList[i]=UserIDList[i].split(".")[0].strip()
    

df = pd.DataFrame(UserIDList) 
df.to_csv("Userlist.csv", index=False)

# joined_files = os.path.join(updatedpath, "*.csv")

# joined_list = glob.glob(joined_files)

# print(joined_list)

# df = pd.concat(map(pd.read_csv, joined_list), ignore_index=True)

# df.to_csv('validation.csv', index=False)

# for name in UserIDList:
#     print(name)
#     try:
#         combined_csv=pd.concat(pd.read_csv(name))
#     except:
#         pass

# combined_csv.to_csv("validation.csv", index=False)