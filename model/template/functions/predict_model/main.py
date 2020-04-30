import os
from datetime import datetime,timezone,timedelta
import pandas as pd
import numpy as np
from google.cloud import storage
from google.cloud import pubsub
import pickle
import glob
from io import BytesIO
import lightgbm
import json
import tempfile

def gcs_upload(data,file_name,bucket):
    _, temp_local_filename = tempfile.mkstemp()
    with open(temp_local_filename,'w', encoding = 'utf-8') as f:
        f.write(data)

    blob = bucket.blob(file_name)
    blob.upload_from_filename(temp_local_filename)
    
    os.remove(temp_local_filename)

def predict_race_dital_get(data):
    #race_idを抽出
    race_id_set = data.race_id.unique()
    race_info_set = {}
    cate_place = {0:'東京',1:'中山',2:'阪神',3:'京都',4:'小倉',5:'新潟',6:'福島',7:'中京',8:'札幌',9:'函館'}
    #race_idごとに馬情報をまとめる
    for race_id in race_id_set:
        horse = {}
        horse_set = data[data["race_id"]==race_id][["umaban","horse_id","horse_name"]]
        for horse_data in horse_set.drop_duplicates(subset=["umaban","horse_id","horse_name"]).values:
            horse[horse_data[0]] = [horse_data[1],horse_data[2]]
        race_set  = data[data["race_id"]==race_id][["race_num","race_name","place"]]
        race_set = race_set.drop_duplicates(subset=["race_num","race_name","place"]).values
        
        race_num = race_set[0][0]
        race_name = race_set[0][1]
        place = cate_place.get(race_set[0][2])
        race = {"race_num":race_num,"race_name":race_name,"place":place}
        race_info_set[race_id] = {"race":race,"horse":horse}
    return race_info_set


def pred_result(data):
    #modelの最終的な予測を計算
    return xxx

def predict_model(model_set,pred_data,n_class):
    #modelの予測を行う


    return xxx
    
def load_model(blobs):
    gbm_set = []
    for blob in blobs:
        bucket = blob.bucket
        blob = bucket.get_blob(blob.name)
        gbm_set.append(pickle.loads(blob.download_as_string()))
    return gbm_set
    

def predict_model(event, context):
    project_id = os.getenv("GCP_PROJECT")
    client = storage.Client(project_id)
    file_name  = event['name']
    buket_name = event['bucket']
    bucket= client.get_bucket(buket_name)
    blob = bucket.get_blob(file_name)
    up_data = blob.download_as_string()
    featuer_data = pd.read_csv(BytesIO(up_data))
    #レース情報の作成
    race_info = predict_race_dital_get(featuer_data)
    '''
    modelの予測を記載



    '''
    topic_name = os.getenv('TOPIC_NAME')
    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    '''
    modelの予測値と最終的な結果をGCSにアップロードする
    '''
        pred_race_data_set.append(json.dumps(pred_race_data, ensure_ascii=False))
        JST = timezone(timedelta(hours=+9), 'JST')  
        race_date = datetime.now(JST)
        tweet_msg = "{}::{} {}R {}\n◎ {}:{}\n○ {}:{}\n▲ {}:{}\n△ {}:{}\n△ {}:{}\n△ {}:{}\n☆ {}:{}\n".format(race_date.strftime('%Y/%m/%d'),race_place,race_num,race_name,no1,horse_set[no1][1],no2,horse_set[no2][1],no3,horse_set[no3][1],no4,horse_set[no4][1],no5,horse_set[no5][1],no6,horse_set[no6][1],no7,horse_set[no7][1])
        #結果をpublish
        future = publisher.publish(topic_path, data=tweet_msg.encode('utf-8'))
    blob_p = client.get_bucket(os.getenv("PRED_RESULT_BUCKET"))
    horse_pred_json_file = 'horse/{}/pred_horse_{}.json'.format(race_date.strftime('%Y%m%d'),race_date.strftime('%Y%m%d%H%M%S'))
    race_pred_json_file = 'race/{}/pred_race_{}.json'.format(race_date.strftime('%Y%m%d'),race_date.strftime('%Y%m%d%H%M%S'))

    pred_horse_data_set = "\n".join(pred_horse_data_set)
    if len(pred_race_data_set) < 2:
        pred_race_data_set = pred_race_data_set[0]
    else:
        pred_race_data_set = "\n".join(pred_race_data_set)
        
    gcs_upload(pred_horse_data_set,horse_pred_json_file,blob_p)
    gcs_upload(pred_race_data_set,race_pred_json_file,blob_p)


    return 'Done'