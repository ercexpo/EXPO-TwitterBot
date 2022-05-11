import csv
import pandas as pd
from datetime import datetime
import json
from collections import defaultdict
import sqlite3
from tqdm.auto import tqdm

def CreateTable():
    conn=sqlite3.connect('database.db')
    c=conn.cursor()
    c.execute("""CREATE TABLE users (
    userid TEXT,
    Validation TEXT,
    Bot_Assigned TEXT,
    last_replied TEXT,
    sinceid TEXT
    );
    """)

    print("Table Created Successfully")
    conn.commit()
    conn.close()

def PopulateTableforTest():

    now = datetime.now()
    dt_string = now.strftime("%Y/%m/%d %H:%M:%S")
 
    print("now =", dt_string)


    df=pd.read_csv('UserDatabase.csv')

    names=df.iloc[:,0].to_list()

    corrected_names=[]
    treatment_group=[]
    last_replied=[]
    bot_assigned=[]
    sinceid=[]
    

    for name in names:
        name=name.split('.csv')[0]
        corrected_names.append(name)
        treatment_group.append('True')
        last_replied.append(dt_string)
        bot_assigned.append('All')
        sinceid.append('12345')

    for i in tqdm(range(len(corrected_names))):
        conn=sqlite3.connect('database.db')
        c=conn.cursor()

        mytuple=(corrected_names[i], treatment_group[i], last_replied[i], bot_assigned[i], sinceid[i])

        sqlite_insert_with_param = """INSERT INTO users
                          VALUES (?, ?, ?, ?, ?)"""

        c.execute(sqlite_insert_with_param, mytuple)

        print("Table Updated Successfully")

        conn.commit()
        conn.close()


def ViewTable():
    conn=sqlite3.connect('database.db')
    c=conn.cursor()
    c.execute("SELECT * FROM users")
    print(c.fetchmany(5))
    conn.commit()
    conn.close()




if __name__ == "__main__":
    #CreateTable()
    #PopulateTableforTest()
    #ViewTable()
    a=[]








# d= defaultdict(list)

# now = datetime.now()
# dt_string = now.strftime("%Y/%m/%d %H:%M:%S")
 
# print("now =", dt_string)


# df=pd.read_csv('UserDatabase.csv')

# names=df.iloc[:,0].to_list()

# corrected_names=[]
# treatment_group=[]
# last_replied=[]
# combined=[]

# for name in names:
#     name=name.split('.csv')[0]
#     corrected_names.append(name)
#     treatment_group.append(True)
#     last_replied.append(dt_string)

# for i in range(len(corrected_names)):
#     combo=[]
#     combo.append(treatment_group[i])
#     combo.append(last_replied[i])
#     combined.append(combo)

# for name in corrected_names:
#     i=0
#     d[name].append(combined[i])
#     i=i+1

# print(d['31285806'][0][1])

# with open('Database.txt', 'a') as convert_file:
# 				convert_file.write(json.dumps(d)) 



# print(corrected_names)





 