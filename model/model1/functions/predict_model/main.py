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
    p1 = data["m1"]
    p2 = data["m2"]
    p3 = data["m3"]
    j = data["m4"]
    re1 = j[0]*1.5+(p1[0]+p1[1]+p2[0]+p2[1]+p3[0])
    re2 = j[1]*1.5+(p1[0]+p1[1]+p1[2]+p2[0]+p2[1]+p2[2]+p3[0])
    re3 = j[0]+j[1]
    re4 = (p1[-2]+p1[-1]+j[-1])/3
    re5 = (p2[-2]+p2[-1]+j[-1])/3
    re6 = ((1-p3[0])+j[-1])/2
    re7 = (p1[-2]+p1[-1]+p2[-2]+p2[-1]+(1-p3[0])+j[-1])
    return re1,re2,re3,re4,re5,re6,re7

def first_model(model_set,pred_data,n_class):
    #結果をまとめるdict
    result_dict = {}
    waku_data = pred_data[:,3]
    pred_data = pred_data[:,3:]
    ave_set = np.zeros((pred_data.shape[0],n_class))
    for i,model in enumerate(model_set):
        pred=model.predict(pred_data)
        
        ave_set += pred.reshape((pred_data.shape[0],n_class))
    
    result_pred = ave_set/10

    return {key:value for key,value in zip(waku_data,result_pred)}
    
def load_model(blobs):
    gbm_set = []
    for blob in blobs:
        bucket = blob.bucket
        blob = bucket.get_blob(blob.name)
        gbm_set.append(pickle.loads(blob.download_as_string()))
    return gbm_set
    

def predict_result(event, context):
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



    #モデル毎に特徴量を作成
    sort_columns=['race_id','horse_id','horse_name','umaban','horse_year','horse_sex','race_rank1','race_rank2','kind','distance','condition','place','p_umaban','p_horse_sex','p_horse_year','p_kinryo','time','last3f','p1','p2','p3','p4', 'p_horse_weight','p_dif_horse_weight','p_race_rank1','p_race_rank2','p_distance','p_kind','p_condition', 'p_place', 'p_horse_count','dif_time','lapf1','lapf2','lapf3','lapl2','lapl1','lotation']
    o1 = featuer_data[featuer_data.past_race==1]#mpdel1の特徴量
    o1 = o1.drop(['top_time','p_wakuban','lap_time','p_race_date','past_race','p_race_id','wakuban','race_date'],axis=1)
    o1 = o1.loc[:, sort_columns]
    #file_name = "{}/order_{}.csv".format(now_time.strftime('%Y%m%d'),now_time.strftime('%H%M%S'))

    o2 = featuer_data[featuer_data.past_race==2]#model2の特徴量
    o2 = o2.drop(['top_time','p_wakuban','lap_time','p_race_date','past_race','p_race_id','wakuban','race_date'],axis=1)
    o2 = o2.loc[:, sort_columns]
    #file_name = "{}/order_{}.csv".format(now_time.strftime('%Y%m%d'),now_time.strftime('%H%M%S'))

    join_key =['race_id', 'umaban','horse_id','horse_name', 'horse_year', 'horse_sex', 'race_rank1', 'race_rank2', 'place', 'distance','kind']
    o3 = o1.merge(o2,on=join_key,how='left')
    del featuer_data
    #それぞれのモデルの結果を格納
    race_pred_set_m1 = {}
    race_pred_set_m2 = {}
    race_pred_set_m3 = {}

    #predデータはレースIDごと渡す.
    blobs =  client.list_blobs("horse_model_1")
    gbm_set = load_model(blobs)
    for p_id in race_info.keys():
        #model1
        pred_m1 = o1[o1["race_id"]==p_id]
        first = first_model(gbm_set,pred_m1.values,5)
        race_pred_set_m1[p_id] = first
    del gbm_set

    blobs =  client.list_blobs("horse_model_2")
    gbm_set = load_model(blobs)
    for p_id in race_info.keys():
        #model1
        pred_m1 = o2[o2["race_id"]==p_id]
        first = first_model(gbm_set,pred_m1.values,5)
        race_pred_set_m2[p_id] = first
    del gbm_set

    blobs =  client.list_blobs("horse_model_3")
    gbm_set = load_model(blobs)
    for p_id in race_info.keys():
        #model1
        pred_m1 = o3[o3["race_id"]==p_id]
        first = first_model(gbm_set,pred_m1.values,1)
        race_pred_set_m3[p_id] = first
    del gbm_set
    #joinモデルの特徴量作成

    
    featuer2 ={}
    for p_id in race_info.keys():
        featuer_set_all = []
        horses = race_info[p_id]["horse"]
        for h in horses.keys():
            featuer = []
            featuer_set = {}
            if p_id in race_pred_set_m1:
                p1=race_pred_set_m1[p_id]
                if h in p1:
                    featuer.append(p1[h].tolist())
                else:
                    featuer.append([0,0,0,0.005,0.995])
            else:
                featuer.append([0,0,0,0.005,0.995])
            
            if p_id in race_pred_set_m2:
                p2=race_pred_set_m2[p_id]
                if h in p2:
                    featuer.append(p2[h].tolist())
                else:
                    featuer.append([0,0,0,0.005,0.995])
            else:
                featuer.append([0,0,0,0.005,0.995])
            
            if p_id in race_pred_set_m3:
                p3=race_pred_set_m3[p_id]
                if h in p3:
                    featuer.append(p3[h].tolist())
                else:
                    featuer.append([0.1])
            else:
                featuer.append([0.1])
            featuer_set["umaban"] = {h:horses[h]}
            featuer_set["train"] = featuer
            featuer_set_all.append(featuer_set)
        featuer2[p_id] = featuer_set_all

    top1 = {}
    top2 = {}
    top3 = {}
    top4 = {}
    top5 = {}
    top6 = {}
    top7 = {}
    topic_name = os.getenv('TOPIC_NAME')
    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    pred_race_data_set = []
    pred_horse_data_set = []
    for race_id in featuer2.keys():
        race_data = race_info[race_id]["race"]
        race_name = race_data["race_name"]
        race_num  = race_data["race_num"]
        race_place = race_data["place"]
        featuer_set = featuer2[race_id]
        horse_set = race_info[race_id]["horse"]
        pred_race_data = {}
        #join modelの予測
        pred_data_set=[]
        for f in featuer_set:
            pred_data_set.append(np.sum(f["train"],axis=0))
        pred_data_set = np.array(pred_data_set)
        ave_set = np.zeros((pred_data_set.shape[0],3))
        blobs =  client.list_blobs("horse_model_join")
        model_set = load_model(blobs)

        for i,model in enumerate(model_set):
            pred=model.predict(pred_data_set)
            ave_set += pred.reshape((pred_data_set.shape[0],3))
        result_pred = ave_set/5
        del model_set
        top1 = {}
        top2 = {}
        top3 = {}
        top4 = {}
        top5 = {}
        top6 = {}
        top7 = {}
        
        for i,j in zip(featuer_set,result_pred):
            pred_horse_data = {}
            race_pred_data_f = {}
            horse = i["umaban"]
            horse_id =  list(horse.values())[0][0]
            race_pred_data_f["m1"] = i["train"][0]
            race_pred_data_f["m2"] = i["train"][1]
            race_pred_data_f["m3"] = i["train"][2]
            race_pred_data_f["m4"] = j.tolist()
            re1,re2,re3,re4,re5,re6,re7 = pred_result(race_pred_data_f)
            top1[list(horse.keys())[0]] = re1
            top2[list(horse.keys())[0]] = re2
            top3[list(horse.keys())[0]] = re3
            top4[list(horse.keys())[0]] = re4
            top5[list(horse.keys())[0]] = re5
            top6[list(horse.keys())[0]] = re6
            top7[list(horse.keys())[0]] = re7
            pred_horse_data["race_id"] = int(race_id)
            pred_horse_data["horse_id"]= horse_id
            pred_horse_data["pred_model_1"] = i["train"][0]
            pred_horse_data["pred_model_2"] = i["train"][1]
            pred_horse_data["pred_model_3"] = i["train"][2]
            pred_horse_data["pred_model_4"] = j.tolist()
            pred_horse_data["result1"]= re1
            pred_horse_data["result2"]= re2
            pred_horse_data["result3"]= re3
            pred_horse_data["result4"]= re4
            pred_horse_data["result5"]= re5
            pred_horse_data["result6"]= re6
            pred_horse_data["result7"]= re7
            pred_horse_data_set.append(json.dumps(pred_horse_data, ensure_ascii=False))
            
        select_horse = []
        no1 = sorted(top1.items(), key=lambda x: x[1])[-1][0]
        select_horse.append(no1)
        
        top2 ={k: v for k, v in top2.items() if k not in select_horse}
        no2 = sorted(top2.items(), key=lambda x: x[1])[-1][0]
        select_horse.append(no2)
        
        top3 ={k: v for k, v in top3.items() if k not in select_horse}
        no3 = sorted(top3.items(), key=lambda x: x[1])[-1][0]
        select_horse.append(no3)
        
        top4 ={k: v for k, v in top4.items() if k not in select_horse}
        no4 = sorted(top4.items(), key=lambda x: x[1])[0][0]
        select_horse.append(no4)
        
        top5 ={k: v for k, v in top5.items() if k not in select_horse}
        no5 = sorted(top5.items(), key=lambda x: x[1])[0][0]
        select_horse.append(no5)
        
        top6 ={k: v for k, v in top6.items() if k not in select_horse}
        no6 = sorted(top6.items(), key=lambda x: x[1])[0][0]
        select_horse.append(no6)
        
        top7 ={k: v for k, v in top7.items() if k not in select_horse}
        no7 = sorted(top7.items(), key=lambda x: x[1])[0][0]
        
        pred_race_data["race_id"] = int(race_id)
        pred_race_data["no1"] = no1
        pred_race_data["no2"] = no2
        pred_race_data["no3"] = no3
        pred_race_data["no4"] = no4
        pred_race_data["no5"] = no5
        pred_race_data["no6"] = no6
        pred_race_data["no7"] = no7
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