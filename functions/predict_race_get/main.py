import datetime
import time
import re
from bs4 import BeautifulSoup
import requests
import json
import os
import base64
import tempfile
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import pubsub

def target_race(data):
    race_bool = False
    #予測対象のレースの条件にあてはまる値を格納したlist
    dis_set = [1200,1400,1600,1800,2000,2200,2400,2600]
    min_horse_count = 11
    rank1_set = ['3歳','3歳以上','4歳以上']
    rank2_set =['未勝利','1勝クラス','2勝クラス','3勝クラス','オープン']
    #条件を満たしているか   
    if (data["distance"] in dis_set) and (data["horse_count"] > min_horse_count) and (data["race_rank1"] in rank1_set) and (data["race_rank2"] in rank2_set):
        race_bool = True
    return race_bool

def predict_race_get(event, context):
    project_name = os.getenv('GCP_PROJECT')#環境変数
    #マシン時間がUTC時刻のため日本時間に変更
    run_day = datetime.datetime.today()
    run_day_j= run_day.astimezone(datetime.timezone(datetime.timedelta(hours=+9)))
    bucket_dir = datetime.datetime.strftime(run_day_j, '%Y%m%d')
    #tempfile
    _, temp_local_filename = tempfile.mkstemp()
    
    #バケット名を指定してbucketを取得
    client = storage.Client(project_name)
    #予測対象のレースファイルをアップロードするバケット
    bucket_name_pred = os.getenv('BUCKET_NAME_PRED_RACE')
    bucket_pred_race = client.get_bucket(bucket_name_pred)
    #レースのURL(publishのデータから)
    pub_msg = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    #以下forに変更
    target_race_list = []
    horse_url_set    = []
    target_file = 'target.json'#last
    for pub in pub_msg:
        pub_url = pub['race_url']
        #取得したレースURLに接続してBeautifulsoupで解析する
        rr = requests.get(pub_url)
        html_u = rr.content
        soup = BeautifulSoup(html_u, 'html.parser') 
        #ファイル出力内容
        info_dict = {}
        race_id_set = []
        keys = ['race_id','race_day','race_start_time','race_name','race_num','race_rank1','race_rank2','place','distance','kind','horse_count','url']
        #レースランクを正規表現で取得するためのパターン
        p_rank2   = r'(未勝利|[1１]+勝クラス|[2２]勝クラス|[3３]勝クラス|オープン)'
        p_rank1   = r'(.歳以上|.歳)'#レース情報
        id_pattern = r"race_id=[0-9]+"
        race_url_pattern = r'/race/[0-9]+' 
        race_url ="https://db.netkeiba.com" 
        race_time_pattern = r'[0-9]+:[0-9]+'

        race_info = soup.find(class_="RaceData02").find_all('span')
        # レース番号
        race_num = soup.find(class_="RaceNum").string.replace('R','')
        # レース名
        race_name = soup.find(class_="RaceName").text.replace('\n','')
        #開催場所
        place = race_info[1].string
        #芝・ダ　距離
        race_dis_kind = soup.find(class_="RaceData01").find('span').string.replace('m','').replace(' ','')  
        #レース開始時刻
        race_time = soup.find(class_="RaceData01").text
        race_time = re.search(race_time_pattern,race_time).group(0)
        #芝・ダ
        race_kind = race_dis_kind[0]
        #距離
        race_dis  = race_dis_kind[1:]
        #レースID
        race_id = re.search(id_pattern,pub_url).group().replace('race_id=','')
        #出走頭数
        count =  race_info[-2].string.replace('頭','')
        #rankを取得する
        if bool(re.search(p_rank1,race_name)) & bool(re.search(p_rank2,race_name)):
            race_rank1=re.search(p_rank1,race_name).group(0)
            race_rank2=re.search(p_rank2,race_name).group(0)
        else:
            #馬柱からレースランクを取得
            race_rank = soup.find(class_="RaceData02").get_text()
            race_rank1=re.search(p_rank1,race_rank).group(0).translate(str.maketrans({'１': '1', '２': '2','３':'3'}))
            race_rank2=re.search(p_rank2,race_rank).group(0).translate(str.maketrans({'４': '4', '２': '2','３':'3'}))

        values = []
        values.append(race_id)
        values.append(datetime.datetime.today().strftime("%Y-%m-%d"))
        values.append(race_time)
        values.append(race_name)
        values.append(int(race_num))
        values.append(race_rank1)
        values.append(race_rank2)
        values.append(place)
        values.append(int(race_dis))
        values.append(race_kind)
        values.append(int(count))
        values.append(pub_url.replace('shutuba_past','shutuba'))
        data = {k: v for k, v in zip(keys, values)}
        
        race_bool = target_race(data)
        #馬情報
        if race_bool:
            get_f = 'GET' 
            print("RaceID:{}:{} {}R:::status({})".format(race_id,place,race_num,get_f))
            umabasira = soup.find_all("div",class_="Horse02")
            horse_list = []
            #target.jsonの書き込み用
            target_race_list.append(json.dumps(data, ensure_ascii=False))
            for i in umabasira:
                #馬情報(URL)をまとめる
                horse_data = {}
                horse_url=i.find("a").get("href")
                horse_id = horse_url.replace(race_url,'').replace('/','').replace('horse','')
                horse_data['horse_id'] = horse_id
                horse_data['horse_url'] = horse_url
                horse_list.append(horse_data)
                data['horse_list'] = horse_list
                horse_url_set.append(horse_url)
            save_file_name_race  = 'target/{}.json'.format(data["race_id"])
            write_data_race = json.dumps(data,ensure_ascii=False).encode('utf-8')
            with open(temp_local_filename,'w', encoding = 'utf-8') as f:
                json.dump(data,f,ensure_ascii=False)
            #GCSに書き込み
            blob = bucket_pred_race.blob(save_file_name_race)
            blob.upload_from_filename(temp_local_filename)
            os.remove(temp_local_filename)
            #write_data_race = "\n".join(write_data_race)
            #gcs_upload(write_data_race,save_file_name_race,bucket_pred_race) 
        else:
            get_f = 'SKIP'
            print("RaceID:{}:{} {}R:::status({})".format(race_id,place,race_num,get_f))
            
    target_race_set = "\n".join(target_race_list)
    with open(temp_local_filename,'w', encoding = 'utf-8') as f:
        f.write(target_race_set)

    blob = bucket_pred_race.blob(target_file)
    blob.upload_from_filename(temp_local_filename)
    os.remove(temp_local_filename)

    publisher = pubsub.PublisherClient()
    topic_name = os.getenv('TOPIC_NAME')
    topic_path = publisher.topic_path(project_name, topic_name)
    horse_url_pub = json.dumps(horse_url_set, ensure_ascii=False).encode('utf-8')
    future = publisher.publish(topic_path, data=horse_url_pub)
    return 'Done'
