import csv
import pandas as pd
from datetime import datetime
import json
from collections import defaultdict

d= defaultdict(list)

now = datetime.now()
dt_string = now.strftime("%Y/%m/%d %H:%M:%S")
 
print("now =", dt_string)


df=pd.read_csv('UserDatabase.csv')

names=df.iloc[:,0].to_list()

corrected_names=[]
treatment_group=[]
last_replied=[]
combined=[]

for name in names:
    name=name.split('.csv')[0]
    corrected_names.append(name)
    treatment_group.append(True)
    last_replied.append(dt_string)

for i in range(len(corrected_names)):
    combo=[]
    combo.append(treatment_group[i])
    combo.append(last_replied[i])
    combined.append(combo)

for name in corrected_names:
    i=0
    d[name].append(combined[i])
    i=i+1

print(d['31285806'][0][1])

with open('Database.txt', 'a') as convert_file:
				convert_file.write(json.dumps(d)) 
# print(corrected_names)



# df = df.apply(lambda x: x.str.split('.csv')[0])

 