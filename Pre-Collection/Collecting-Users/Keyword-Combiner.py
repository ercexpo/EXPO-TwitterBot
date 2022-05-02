from operator import index
import os
import pandas as pd
import csv

path = os.getcwd()


updatedpath= path + '/keyword-results'
print(updatedpath)

keyword_normal_CSV_list=os.listdir(updatedpath)

print(len(keyword_normal_CSV_list))

with open('Sports') as f:
    Sports = f.read().strip().split('\n')

with open('lifestyle') as f:
    lifestyle = f.read().strip().split('\n')

with open('Entertainment') as f:
    Entertainment = f.read().strip().split('\n')

masterpd=pd.DataFrame()
zero_keywords=[]

for name in keyword_normal_CSV_list:
    sep='.'
    keyword_name=name.split(sep, 1)[0]
    try:
        with open("keyword-results/%s" % name) as f:
            df=pd.read_csv(f)
            df=df['full_text']
            print(df.shape)
            count=len(df.index)
            # df=pd.DataFrame(df.values.reshape(1,-1))         
            # print(df.shape)
            #print(df.head)
            #print(count)
            for s in Sports:
                if s == keyword_name:
                    topic= 'Sports'
            for l in lifestyle:
                if l == keyword_name:
                    topic= 'lifestyle'
            
            for e in Entertainment:
                if e == keyword_name:
                    topic= 'Entertainment'
            
            
            
            dict={'Keyword': keyword_name, 'Topic': topic,  'Count': count, 'Tweet1': df.iloc[0], 'Tweet2': df.iloc[1], 'Tweet3': df.iloc[2], 'Tweet4': df.iloc[3], 'Tweet5': df.iloc[4] }
            df1=pd.DataFrame([dict])
            print(df1.head)
            masterpd = pd.concat([masterpd, df1])
    except Exception as e:
        print(e)
        zero_keywords.append(keyword_name)
        continue
     
masterpd.to_csv('master.csv', index=False)

with open('zero_keywords', "w") as f:
    f.write(str(zero_keywords))
