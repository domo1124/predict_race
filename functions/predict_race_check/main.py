from google.cloud import storage
import json
from datetime import datetime,timezone,timedelta
import os
from bs4 import BeautifulSoup
import requests
import re
import tempfile
import csv 

def csv_upload_gcs(header,data,file_name,blob):
    _, temp_local_filename = tempfile.mkstemp()
    with open(temp_local_filename,'w', encoding = 'utf-8') as f:
        writer = csv.DictWriter(f, header)
        writer.writeheader()
        for d in data:
            writer.writerow(d)
    
    blob.upload_from_filename(temp_local_filename)
    os.remove(temp_local_filename)


#targeet.jsonの読み込み
def predict_race_check(request):
    project_id = os.getenv("GCP_PROJECT")
    client = storage.Client(project_id)

    bucket_name_p = os.getenv("BUCKET_NAME_PRED")
    bucket= client.get_bucket(bucket_name_p)

    bucket_name_o = os.getenv('BUCKET_NAME_ORDER')
    bucket_o = client.get_bucket(bucket_name_o)

    blob = bucket.get_blob('target.json')
    JST = timezone(timedelta(hours=+9), 'JST')
    now_time = datetime.now(JST)
    order_data = []
    header=["race_id","race_num","race_name","wakuban","umaban","horse_id","horse_name","horse_year","horse_sex","kinryo","horse_weight","dif_horse_weight","race_date","race_rank1","race_rank2","place","distance","kind","horse_count","condition"]

    for i in blob.download_as_string().decode('utf-8').split('\n'):
        race_data = json.loads(i)
        start_hm = race_data['race_start_time'].split(':')
        race_h = int(start_hm[0])
        race_m = int(start_hm[1])
        start_time = datetime(now_time.year, now_time.month, now_time.day, race_h,race_m,tzinfo=JST)
        dl = (now_time-start_time).total_seconds()
        get_detal = (dl//3600)
        #race_orderのbucketに同IDのファイルが無い場合作成
        file_name_o = "{}/{}.csv".format(now_time.strftime('%Y%m%d'),race_data["race_id"])
        blob_o = bucket_o.blob(file_name_o) 
        if blob.exists():
            print("race_id::{} Done!".format(race_data["race_id"]))
        else:
            if get_detal < 0 and get_detal >=-1:
                #urlに接続
                race_url = race_data["url"]
                rr = requests.get(race_url)
                html_u = rr.content
                order = BeautifulSoup(html_u, 'html.parser')

                p_years = r'([0-9]+)'
                p_sex   = r'(牝|牡|セ)'
                p_place = r'(東京|中山|阪神|京都|小倉|新潟|福島|中京|札幌|函館)'
                p_condition = r'(良|稍重|重|不良|稍|不)'
                p_rank2   = r'(.歳以上|.歳)'

                result_table = order.find_all("table",class_=re.compile('RaceTable01'))
                #馬体重の更新を確認
                weight_check=order.find_all("td",class_="Weight")
                w_check = 0
                for w in weight_check:
                    if bool(re.search(r'[0-9]+',w.text)):
                        w_check = w_check+1
                #1頭も更新が無い場合
                if w_check == 0:
                    print("not horse weight update::URL::{}".format(race_url))
                else:
                    print("horse weight update::URL::{}".format(race_data['race_name'],race_url))
                    trs = result_table[0].find_all("tr")
                    data = trs[2:]#ヘッダー以外のデータ
                    horse_count = len(data) 
                    
                    #馬場状態
                    race_condition = [re.search(p_condition,con.text).group(0) for con in order.find(class_="RaceData01").find_all('span') if bool(re.search(p_condition,con.text))]
                    if len(race_condition) !=0:
                        race_condition=race_condition[0]
                    else:
                        race_condition=None
                    horse_order = [] 
                    for tr in data:
                        horse_data = {}
                        result = list(tr.find_all('td'))
                        waku1 = result[0].text
                        waku2 = result[1].text
                        horse_id = re.search(r'[0-9]+',result[3].find("a").get('href')).group(0)
                        horse_name = result[3].find("a").text
                        year = re.search(r'[0-9]',result[4].text).group(0)
                        sex = re.search(p_sex,result[4].text).group(0)
                        kinryo= result[5].text
                        weight = re.search(r'[0-9]+',result[8].text).group(0)
                        dif_weight = re.search(r'\(.+\)',result[8].text).group(0).replace('(','').replace(')','')
                        horse_data["race_id"] = int(race_data['race_id'])
                        horse_data["race_num"] = int(race_data['race_num'])
                        horse_data["race_name"] = race_data['race_name']
                        horse_data['wakuban']=int(waku1)
                        horse_data['umaban']=int(waku2)
                        horse_data['horse_id']=int(horse_id)
                        horse_data['horse_name']=horse_name
                        horse_data['horse_year']=int(year)
                        horse_data['horse_sex']=sex
                        horse_data['kinryo']=float(kinryo)
                        horse_data['horse_weight']=int(weight)
                        horse_data['dif_horse_weight']=int(dif_weight)
                        horse_data["race_date"] =race_data['race_day']
                        horse_data["race_rank1"] =race_data['race_rank1']
                        horse_data["race_rank2"] =race_data['race_rank2']
                        horse_data["place"] =race_data['place']
                        horse_data["distance"] =race_data['distance']
                        horse_data["kind"] =race_data['kind']
                        horse_data["horse_count"] =race_data['horse_count']
                        horse_data['condition']=race_condition
                        order_data.append(horse_data)
                        horse_order.append(horse_data)
                    csv_upload_gcs(header,horse_order,file_name_o,blob_o)
            else:
                continue
    #予測対象レースが存在するとき出走表データ(CSV)を作成
    if len(order_data) != 0:

        bucket_name_f = os.getenv('BUCKET_NAME_FEATURE')
        bucket_f = client.get_bucket(bucket_name_f)

        file_name = "{}/order_{}.csv".format(now_time.strftime('%Y%m%d'),now_time.strftime('%H%M%S'))
        blob_f = bucket_f.blob(file_name)

        csv_upload_gcs(header,order_data,file_name,blob_f)
    return 'Done'