import sqlite3
import json

#jsonファイルの読みこみ
con = sqlite3.connect('./data/race_data_set.db') # データベースに接続する
cur = con.cursor()
 


 

#同ディレクトリのjson(Bigquery用)を読みこんでInsert

with open("./data/race_result.json",'r',encoding= 'utf-8') as fr:
    for i in fr:
        data=tuple(json.loads(i.strip()).values())
        cur.execute('''insert into race_result values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)

with open("./data/race_info.json",'r',encoding= 'utf-8') as fi:
    for i in fi:
        data=tuple(json.loads(i.strip()).values())
        cur.execute('''insert into race_info_result values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)
    
with open("./data/race_pay.json",'r',encoding= 'utf-8') as fp:
    for i in fp:
        data=tuple(json.loads(i.strip()).values())
        cur.execute('''insert into race_pay values(?,?,?,?,?,?)''', data)

con.commit()
con.close() 