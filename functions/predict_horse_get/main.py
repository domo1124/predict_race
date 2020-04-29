import datetime
import time
import re
from bs4 import BeautifulSoup
import requests
import json
import os
import base64
import tempfile
from google.cloud import storage
from google.cloud import pubsub

def predict_horse_get(event, context):
    project_name = os.getenv('GCP_PROJECT')
    #バケット名を指定してbucketを取得
    client = storage.Client(project_name)
    #予測対象の馬ファイルをアップロードするバケット
    bucket_name_horse = os.getenv('BUCKET_NAME_PRED_HORSE')
    bucket_pred_horse = client.get_bucket(bucket_name_horse) 
    #ファイル確認のバケット
    bucket_name_past = os.getenv('BUCKET_NAME_PAST_RACE')
    #GCS上のファイル一覧
    upload_file_list = set([file.name.replace('.json','') for file in client.list_blobs(bucket_name_past)])    
    #tempfile
    _, temp_local_filename = tempfile.mkstemp()
    #レースのURL(publishのデータから)
    pub_msg = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    race_id_set =[]
    race_url ="https://db.netkeiba.com"
    race_url_pattern = r'/race/[0-9]+'
    p_place = r'(東京|中山|阪神|京都|小倉|新潟|福島|中京|札幌|函館)'
    p_kind =r'(芝|ダ)'
    for pub in pub_msg:
        horse_set = []
        r_horse = requests.get(pub)
        html_horse = r_horse.content
        horse = BeautifulSoup(html_horse, 'html.parser')
        horse_id = pub.replace(race_url,'').replace('/','').replace('horse','')
        horse_json = {}
        result_table = horse.find_all("table",class_="db_h_race_results nk_tb_common")
        #tableが存在しない場合、Break
        if len(result_table) ==0:
            continue
        trs = result_table[0].find_all("tr")[1:]
        for tr in trs:
            # 1行ごとにtd, tr要素のデータを取得
            race_day  = tr.find('td').text
            race_place= re.search(p_place,tr.find_all('td')[1].text).group(0) if re.search(p_place,tr.find_all('td')[1].text) else 'None'
            race_kind = re.search(p_kind,tr.find_all('td')[14].text).group(0) if re.search(p_kind,tr.find_all('td')[14].text) else 'None'
            for cell in tr.findAll(['td', 'th']):
            #a hrefの要素が含まれているセルを絞りこむ
                urls = cell.find('a')
                if bool(re.search(p_place,race_place)) and bool(re.search(p_kind,race_kind)):
                    if urls:
                        r_url=re.match(race_url_pattern,cell.find('a').attrs['href'])
                        if r_url:
                            r_url_str = r_url.group()
                            p_race_id = r_url_str.replace('/','').replace('race','')
                            horse_json['horse_id'] = horse_id
                            horse_json['race_id']  = p_race_id
                            horse_json['race_day'] = race_day
                            race_id_set.append(p_race_id)
                            horse_set.append(json.dumps(horse_json, ensure_ascii=False))
                            break
                        else:
                            continue
        horse_json_file = '{}.json'.format(horse_id)
        horse_set = "\n".join(horse_set)
        with open(temp_local_filename,'w', encoding = 'utf-8') as f:
            f.write(horse_set)
        
        blob = bucket_pred_horse.blob(horse_json_file)
        blob.upload_from_filename(temp_local_filename)
        os.remove(temp_local_filename)
    #GCSに存在しないレースデータのIDをPublish
    race_id_set = set(race_id_set)
    pub_list = list(race_id_set - upload_file_list)
    publisher = pubsub.PublisherClient()
    #publishするTopic名
    topic_name = os.getenv('TOPIC_NAME')
    topic_path = publisher.topic_path(project_name, topic_name)
    #topicにpublish
    if len(pub_list) == 0:
        return 'not get race'
    else:
        for i in pub_list:
            future = publisher.publish(topic_path, data=i.encode('utf-8'))

    return 'Done'