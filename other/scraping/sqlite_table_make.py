import sqlite3
import json


#sqliteのテーブル作成

result_table = """

              CREATE TABLE 
              "race_result" 
              ( race_id INTEGER, 
                tyaku INTEGER, 
                wakuban INTEGER,
                umaban INTEGER,
                horse_id INTEGER, 
                horse_name TEXT, 
                horse_sex TEXT,
                horse_years INTEGER, 
                jockey TEXT, 
                kinryo REAL,
                odds REAL, 
                popular INTEGER,
                time RREAL,
                last3f REAL,  
                p1 INTEGER, 
                p2 INTEGER, 
                p3 INTEGER, 
                p4 INTEGER, 
                horse_weight INTEGER, 
                dif_horse_weight INTEGER )
              """
info_table = """
             CREATE TABLE 
             "race_info" 
             ( race_id INTEGER, 
               race_num INTEGER,
               race_name TEXT,
               race_date TEXT,   
               race_rank1 TEXT,
               race_rank2 TEXT, 
               distance INTEGER,
               kind TEXT ,
               condition TEXT,
               place TEXT ,  
               horse_count INTEGER,
               lap_time TEXT,
               conner1 TEXT,
               conner2 TEXT,
               conner3 TEXT,
               conner4 TEXT
                )
             """

pay_table = """
            CREATE TABLE
            "race_pay" 
            ( race_id INTEGER,
              indicator TEXT,
              horse_1 TEXT,
              horse_2 TEXT,
              horse_3 TEXT,
              pay INTEGER
              )
            """


#jsonファイルの読みこみ
con = sqlite3.connect('./data/race_data_set.db') # データベースに接続する
cur = con.cursor()
 

cur.execute(result_table)
cur.execute(info_table)
cur.execute(pay_table) 
con.commit()

#同ディレクトリのjson(Bigquery用)を読みこんでInsert

with open("race_result.json",'r',encoding= 'utf-8') as fr:
    for i in fr:
        data=tuple(json.loads(i.strip()).values())
        cur.execute('''insert into race_result values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)

with open("race_info.json",'r',encoding= 'utf-8') as fi:
    for i in fi:
        data=json.loads(i.strip())
        data["kind"] = data["kind"].replace(" ","").replace("ダート","ダ")
        data = tuple(data.values())
        cur.execute('''insert into race_info values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)
    
with open("race_pay.json",'r',encoding= 'utf-8') as fp:
    for i in fp:
        data=tuple(json.loads(i.strip()).values())
        cur.execute('''insert into race_pay values(?,?,?,?,?,?)''', data)

con.commit()
 
con.close() # 接続を閉じる
