import numpy as np
import pandas as pd
import sqlite3

#最初のコーナー順位を取り出す
def first_past(data):
    past = 0
    for i in data:
        if i !=0:
            past=i
            break
    return past

#距離分類
def distance_category(x):
    if x <= 1200:
        cate = 0
    elif x <= 1400:
        cate = 1
    elif x <= 1600:
        cate = 2
    elif x <= 1800:
        cate = 3
    elif x <= 2000:
        cate = 4
    elif x <= 2200:
        cate = 5
    elif x<=2400:
        cate = 6
    else:
        cate = 7
    return cate

#sqlite 設定
con = sqlite3.connect('./data/race_data_set.db')
con.enable_load_extension(True)
con.load_extension("../../sqlite-amalgamation-3260000/libsqlitefunctions.so")
cur = con.cursor()
cur = con.cursor()

sql_result = "select race_id,stdev(last3f) as std3f,avg(last3f) as ave3f from race_result group by race_id"
sql_info   = "select * from race_info"
sql_pay = "select * from race_pay"

df_r = pd.read_sql_query(sql=sql_result,con=con)
df_i = pd.read_sql_query(sql=sql_info,con=con)

df = pd.read_csv("./data/horse_data_set.csv")
df["pred_race_date"] = pd.to_datetime(df["pred_race_date"],format='%Y-%m-%d')
df["past_race_date"] = pd.to_datetime(df["past_race_date"],format='%Y-%m-%d')
df = df.merge(df_r,right_on="race_id",left_on="past_race_id",how="left")

#last3f偏差値計算
df["h_last3f"] = (((df["past_last3f"]-df["ave3f"])*10)/df["std3f"])*-1+50
del_col = ['race_id','ave3f', 'std3f']
df = df.drop(columns=del_col)

#性別のカテゴライズ
cate_sex  = {'牝':0,'牡':1,'セ':2}
df['pred_horse_sex'] = df['pred_horse_sex'].apply(lambda x : cate_sex.get(x))
df['past_horse_sex'] = df['past_horse_sex'].apply(lambda x : cate_sex.get(x))

#競馬場のカテゴライズ
cate_place = {'東京':0,'中山':1,'阪神':2,'京都':3,'小倉':4,'新潟':5,'福島':6,'中京':7,'札幌':8,'函館':9}
df['pred_place'] = df['pred_place'].apply(lambda x : cate_place.get(x))
df['past_place'] = df['past_place'].apply(lambda x : cate_place.get(x))

#馬場状態のカテゴライズ
cate_condition ={'良':0,'稍重':1,'稍':1,'重':2,'不良':3,'不':3}
df['pred_condition'] = df['pred_condition'].apply(lambda x : cate_condition.get(x))
df['past_condition'] = df['past_condition'].apply(lambda x : cate_condition.get(x))

#レースランク1のカテゴライズ
cate_rank1 = {'2歳':0,'3歳':1,'3歳以上':2,'4歳以上':3}
df['pred_race_rank1'] = df['pred_race_rank1'].apply(lambda x : cate_rank1.get(x))
df['past_race_rank1'] = df['past_race_rank1'].apply(lambda x : cate_rank1.get(x))
#レースランク2のカテゴライズ
cate_rank2   = {'新馬':0,'未勝利':1,'500万下':2,'1000万下':3,'1600万下':4,'オープン':5,'1勝クラス':2,'2勝クラス':3,'3勝クラス':4,}
df['past_race_rank2'] = df['past_race_rank2'].apply(lambda x : cate_rank2.get(x))
df['pred_race_rank2'] = df['pred_race_rank2'].apply(lambda x : cate_rank2.get(x))

#コースの種類のカテゴライズ
cate_kind = {'芝':0,'ダ':1}
df['pred_kind'] = df['pred_kind'].apply(lambda x : cate_kind.get(x))
df['past_kind'] = df['past_kind'].apply(lambda x : cate_kind.get(x))


df["first_past"] = df[["past_p1","past_p2","past_p3","past_p4"]].apply(lambda x: first_past(x),axis=1)

#位置取りの計算
df['dif_past'] = df["past_tyaku"] / df["past_p4"]

df["first_past"] = df["first_past"] / df["past_horse_count"]
df['past_p1'] = df["past_p1"] / df["past_horse_count"]
df['past_p2'] = df["past_p2"] / df["past_horse_count"]
df['past_p3'] = df["past_p3"] / df["past_horse_count"]
df['past_p4'] = df["past_p4"] / df["past_horse_count"]
df['past_pg'] = df["past_tyaku"] / df["past_horse_count"]
df['dif_past2'] = df["past_p3"] - df["past_p4"]

#ローテーションの計算
df['lotation'] = (df["pred_race_date"] - df["past_race_date"]).dt.days

#ラップタイムの変換
df["lapf1"] = None
df["lapf2"] = None
df["lapf3"] = None
df["lapf4"] = None
df["lapl3"] = None
df["lapl2"] = None
df["lapl1"] = None
df["dif_lap21"] = None
df["dif_lap32"] = None
df["dif_lap43"] = None
df["race_dif_3f"] = None
df["race_first_3f"] = None
df["race_last_3f"] = None
df[["lapf1","lapf2","lapf3","lapl3","lapf4","lapl2","lapl1"]] = [np.array(x.split('-'),dtype='float64')[[0,1,2,3,-3,-2,-1]] for x in df.past_lap_time]
df["lapf1"] = [x[0]/200.0 if (x[1]/100)%2 == 0 else x[0]/100.0 for x in df[["lapf1","past_distance"]].values]
df[["lapf2","lapf3","lapf4","lapl3","lapl2","lapl1"]] = df[["lapf2","lapf3","lapf4","lapl3","lapl2","lapl1"]]/200.0
df["race_first_3f"] = df["lapf1"]+ df["lapf2"] + df["lapf3"]
df["race_last_3f"]  = df["lapl1"]+ df["lapl2"] + df["lapl3"]
df["dif_lap21"] = df["lapf2"] - df["lapf1"]
df["dif_lap32"] = df["lapf3"] - df["lapf2"]
df["dif_lap43"] = df["lapf4"] - df["lapf3"]
df["race_dif_3f"] = df["race_first_3f"]-df["race_last_3f"]
df = df.drop(columns=["lapf4"])

#距離延長、短縮
df["dif_distance"] = (df["pred_distance"] - df["past_distance"])/200



df["pred_distance"] = df["pred_distance"].apply(lambda x : distance_category(x))
df["past_distance"] = df["past_distance"].apply(lambda x : distance_category(x))

del_col = ['past_race_id','horse_name','pred_wakuban','pred_race_date','past_tyaku','past_wakuban','past_race_date', 'past_horse_count', 'past_lap_time']
df = df.drop(columns=del_col)
df.to_csv("./data/baseline_data.csv",index=False)
