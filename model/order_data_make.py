import numpy as np
import pandas as pd
import sqlite3

def order_data_make(i): 
    data = i[1]
    data= data[data["p_race_date_y"]<i[0][1]]
    data=data.sort_values(["p_umaban_x","p_race_date_y"], ascending=[True, False])
    comb = data.groupby(["p_umaban_x"]).grouper.group_info[0]
    count=[];nth=0
    for i,c in enumerate(comb):
        if i ==0 or comb[i-1] !=c:
            nth=1
        else:
            nth +=1
        count += [nth]
    data["counts"] = count
    #data = data[data["counts"]<4]
    return data
#いらないcolumnのlist
del_columns =[ 
 'horse_name_y',
 'top_time']
rename_column =[
 'pred_race_id',
 'horse_id',
 'horse_name',
 'pred_horse_years',
 'pred_horse_sex',
 'pred_tyaku',
 'pred_wakuban',
 'pred_umaban',
 'pred_race_date',
 'pred_race_rank1',
 'pred_race_rank2',
 'pred_kind',
 'pred_distance',
 'pred_condition',
 'pred_place',
 'past_race_id',
 'past_tyaku',
 'past_wakuban',
 'past_umaban',
 'past_horse_sex',
 'past_horse_years',
 'past_kinryo',
 'past_time',
 'past_last3f',
 'past_p1',
 'past_p2',
 'past_p3',
 'past_p4',
 'past_horse_weight',
 'past_dif_horse_weight',
 'past_race_date',
 'past_race_rank1',
 'past_race_rank2',
 'past_distance',
 'past_kind',
 'past_condition',
 'past_place',
 'past_horse_count',
 'past_lap_time',
 'past_dif_time',
 'past_race_num'
]
sort_columns=[
 'pred_race_id',
 'past_race_id',
 'horse_id',
 'horse_name',
 'past_race_num',
 'pred_tyaku',
 'pred_wakuban',
 'pred_umaban',
 'pred_horse_years',
 'pred_horse_sex',
 'pred_race_date',
 'pred_race_rank1',
 'pred_race_rank2',
 'pred_kind',
 'pred_distance',
 'pred_condition',
 'pred_place',
 'past_tyaku',
 'past_wakuban',
 'past_umaban',
 'past_horse_sex',
 'past_horse_years',
 'past_kinryo',
 'past_time',
 'past_last3f',
 'past_p1',
 'past_p2',
 'past_p3',
 'past_p4',
 'past_horse_weight',
 'past_dif_horse_weight',
 'past_race_date',
 'past_race_rank1',
 'past_race_rank2',
 'past_distance',
 'past_kind',
 'past_condition',
 'past_place',
 'past_horse_count',
 'past_lap_time',
 'past_dif_time'
]
query = """
            select *,round(time-top_time,2) as dif_time from (
            SELECT 
                rr.race_id as p_race_id, 
                rr.tyaku as p_tyaku,
                rr.wakuban as p_wakuban,
                rr.umaban as p_umaban,
                rr.horse_id,
                rr.horse_name,
                rr.horse_sex as p_horse_sex,
                rr.horse_years as p_horse_years,
                rr.kinryo as p_kinryo,
                rr.time,
                min(rr.time) over(partition by rr.race_id) as top_time, 
                rr.last3f,
                rr.p1,
                rr.p2,
                rr.p3,
                rr.p4,
                rr.horse_weight as p_horse_weight,
                rr.dif_horse_weight as p_dif_horse_weight,
                ri.race_date as p_race_date,
                ri.race_rank1 as p_race_rank1,
                ri.race_rank2 as p_race_rank2,
                ri.distance as p_distance,
                ri.kind as p_kind,
                ri.condition as p_condition,
                ri.place as p_place,
                ri.horse_count as p_horse_count,
                ri.lap_time
                FROM race_result rr left join race_info ri on rr.race_id = ri.race_id )  tmp
"""
#データの読み込み
con = sqlite3.connect('./data/race_data_set.db') # データベースに接続する
cur = con.cursor()
df = pd.read_sql_query(sql=query,con=con)
df["p_race_date"] = pd.to_datetime(df["p_race_date"],format='%Y-%m-%d')
#出走表データの作成
horse_order = df.sort_values(["p_race_id","p_umaban"])[["p_race_id","horse_id","horse_name","p_horse_years","p_horse_sex","p_tyaku","p_wakuban","p_umaban","p_race_date","p_race_rank1","p_race_rank2","p_kind","p_distance","p_condition","p_place"]]
#馬ごとにデータをまとめる
horse_data = df.groupby("horse_id").apply(lambda x:x.sort_values("p_race_date"))
#Merge
merge_data=horse_order.merge(horse_data.set_index(["horse_id"]),on='horse_id',how='left')
#order_dataと過去出走分を連結
order_set = [order_data_make(d) for d in merge_data.groupby(["p_race_id_x","p_race_date_x"])]
df_order=pd.concat(order_set)


learn_data_set= df_order.drop(columns=del_columns)

key_column = list(learn_data_set.columns)
column_dict = {key:value for key,value in zip(key_column,rename_column)}
learn_data_set = learn_data_set.rename(columns=column_dict)
learn_data_set = learn_data_set.loc[:, sort_columns]
learn_data_set.to_csv("./data/horse_data_set.csv", index=False)