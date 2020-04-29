import os
from datetime import datetime
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from io import BytesIO


    
def race_laptime(elm):
    lap = elm.split('-')
    lap = [float(l) for l in lap]
    lap_set = []
    lap_set.append(lap[0])
    lap_set.append(lap[1])
    lap_set.append(lap[2])
    lap_set.append(lap[-2])
    lap_set.append(lap[-1])
    s = pd.Series(lap_set, index=["lapf1","lapf2","lapf3","lapl2","lapl1"])
    return s

def category_set(line):
    cate_sex  = {'牝':0,'牡':1,'セ':2}
    cate_place = {'東京':0,'中山':1,'阪神':2,'京都':3,'小倉':4,'新潟':5,'福島':6,'中京':7,'札幌':8,'函館':9}
    cate_condition ={'良':0,'稍重':1,'稍':1,'重':2,'不良':3,'不':3}
    cate_rank2   = {'新馬':0,'未勝利':1,'500万下':2,'1000万下':3,'1600万下':4,'オープン':5,'1勝クラス':2,'2勝クラス':3,'3勝クラス':4,}
    cate_rank1 = {'2歳':0,'3歳':1,'3歳以上':2,'4歳以上':3}
    cate_kind = {'芝':0,'ダ':1,'ダート':1}
    line['horse_sex'] = cate_sex.get(line["horse_sex"])
    line['p1'] = line["p1"] / line["horse_count"]
    line['p2'] = line["p2"] / line["horse_count"]
    line['p3'] = line["p3"] / line["horse_count"]
    line['p4'] = line["p4"] / line["horse_count"]
    line['race_rank1'] = cate_rank1.get(line["race_rank1"])
    line['race_rank2'] = cate_rank2.get(line["race_rank2"])
    line['kind'] = cate_kind.get(line["kind"])
    line['condition'] = cate_condition.get(line["condition"])
    line['place'] = cate_place.get(line["place"])
    line['p_horse_sex'] = cate_sex.get(line["p_horse_sex"])
    line['p_race_rank1'] = cate_rank1.get(line["p_race_rank1"])
    line['p_race_rank2'] = cate_rank2.get(line["p_race_rank2"])
    line['p_place'] = cate_place.get(line["p_place"])
    line['p_condition'] = cate_condition.get(line["p_condition"])
    line['p_kind'] = cate_kind.get(line["p_kind"].replace(" ",""))
    line['lotation'] = (line["race_date"] - line["p_race_date"]).days
    return line

def featuer_make(event, context):
    project_id = os.getenv("GCP_PROJECT")
    bucket_name = os.getenv("FEATURE_BUCKET_NAME")

    client = storage.Client(project_id)
    file_name  = event['name']
    buket_name = event['bucket']
    bucket= client.get_bucket(buket_name)
    blob = bucket.get_blob(file_name)
    up_data = blob.download_as_string()
    order_data = pd.read_csv(BytesIO(up_data),dtype={"horse_id": int})
    order_data["race_date"]=pd.to_datetime(order_data["race_date"],format='%Y-%m-%d').dt.date

    #horse_idのlistを取得
    horse_id_set = list(order_data.horse_id)
    #BigQueyrのresult_raceテーブル接続
    bgq_client = bigquery.Client()
    #データセット名とテーブル名
    table_info = "race_info"
    table_result="race_result"
    query_params = [bigquery.ArrayQueryParameter("horse_list", "INT64", horse_id_set)]
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_params
    query = """
                WITH predict_race AS (
                SELECT @horse_list AS horse_id) 

                select *,round(time-top_time,2) as dif_time from (
                SELECT 
                    rr.race_id as p_race_id, 
                    rr.horse_id,
                    rr.wakuban as p_wakuban,
                    rr.umaban as p_umaban,
                    rr.horse_sex as p_horse_sex,
                    rr.horse_year as p_horse_year,
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
                    ri.lap_time,
                    row_number() over(partition by horse_id order by ri.race_date desc) as past_race
                FROM `{0}.{1}.{1}` rr left join `{0}.{2}.{2}` ri on rr.race_id = ri.race_id )  tmp 
                where tmp.past_race < 3 and EXISTS　(select horse_id from predict_race,unnest(horse_id) as horse_id where tmp.horse_id = horse_id)
                """.format(project_id, table_result, table_info)#dataset,tableは環境変数


    rows = bgq_client.query(query,job_config=job_config).result().to_dataframe()
    mer = order_data.merge(rows,how='left',on='horse_id')
    mer[["lapf1","lapf2","lapf3","lapl2","lapl1"]] = mer.lap_time.apply(race_laptime)
    mer = mer.apply(category_set,axis=1)
    #ここでCSV出力
    client = storage.Client()
    # バケットオブジェクト取得
    bucket = client.get_bucket(bucket_name)
    # 保存先フォルダとファイル名作成
    file_name = "feature_{}.csv".format(datetime.now().strftime('%Y%m%d%H%M%S'))
    blob = bucket.blob(file_name)
    blob.upload_from_string(data=mer.to_csv(sep=",", index=False), content_type='text/csv')

    return 'Done'
