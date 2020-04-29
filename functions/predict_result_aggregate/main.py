#予測結果の集計関数
from google.cloud import storage
import json
from datetime import datetime,timezone,timedelta
import os
from bs4 import BeautifulSoup
import requests
import re
import tempfile

def gcs_upload(data,file_name,bucket):
    _, temp_local_filename = tempfile.mkstemp()
    with open(temp_local_filename,'w', encoding = 'utf-8') as f:
        f.write(data)

    blob = bucket.blob(file_name)
    blob.upload_from_filename(temp_local_filename)
    
    os.remove(temp_local_filename)
    
def predict_result_aggregate(request):
    project_id = os.getenv("GCP_PROJECT")
    client = storage.Client(project_id)
    bucket_name = os.getenv('BUCKET_NAME')
    bucket= client.get_bucket(bucket_name)
    
    JST = timezone(timedelta(hours=+9), 'JST')
    race_date = datetime.now(JST).strftime("%Y%m%d")
    agg_data_set = []
    agg_list = client.list_blobs(bucket_name,prefix="race/{}".format(race_date))
    for file in agg_list:
        blob = bucket.blob(file.name) 
        for data in blob.download_as_string().decode('utf-8').split('\n'):
            race_data = json.loads(data)
            race_id = race_data["race_id"]
            #払い戻し結果を取得
            race_url = "https://race.netkeiba.com/race/result.html?race_id={}&rf=race_list".format(race_id)
            rr = requests.get(race_url)
            html_u = rr.content
            order = BeautifulSoup(html_u, 'html.parser')
            
            #単勝
            tan_set_u = []
            tan_set_p = []
            tan_data = order.find('tr',class_="Tansho")
            t1 = tan_data.find('td',class_="Result")
            t2 = tan_data.find('td',class_="Payout")
            t1 = t1.find_all('span')
            for i in t1:
                if bool(re.search(r'[0-9]+',i.text)):
                    tan_set_u.append(i.text)
            pay = t2.text.replace(",","").split("円")
            for i in pay:
                if bool(re.search(r'[0-9]+',i)):
                    tan_set_p.append(i)
            
            race_data["tan_u"] = tan_set_u
            race_data["tan_p"] = tan_set_p
            #複勝
            fuku_set_u = []
            fuku_set_p = []
            fuku_data = order.find('tr',class_="Fukusho")
            t1 = fuku_data.find('td',class_="Result")
            t2 = fuku_data.find('td',class_="Payout")
            t1 = t1.find_all('span')
            for i in t1:
                if bool(re.search(r'[0-9]+',i.text)):
                    fuku_set_u.append(i.text)
            pay = t2.text.replace(",","").split("円")
            for i in pay:
                if bool(re.search(r'[0-9]+',i)):
                    fuku_set_p.append(i)
            
            race_data["fuku_u"] = fuku_set_u
            race_data["fuku_p"] = fuku_set_p
            
            #馬連
            uren_set_u = []
            uren_set_p = []
            uren_data = order.find('tr',class_="Umaren")
            t1 = uren_data.find('td',class_="Result")
            t2 = uren_data.find('td',class_="Payout")
            t1 = t1.find_all('span')
            for i in t1:
                if bool(re.search(r'[0-9]+',i.text)):
                    uren_set_u.append(i.text)
            pay = t2.text.replace(",","").split("円")
            for i in pay:
                if bool(re.search(r'[0-9]+',i)):
                    uren_set_p.append(i)
            race_data["umaren_u"] = uren_set_u
            race_data["umaren_p"] = uren_set_p
            #ワイド
            wide_set_u = []
            wide_set_p = []
            wide_data = order.find('tr',class_="Wide")
            t1 = wide_data.find('td',class_="Result")
            t2 = wide_data.find('td',class_="Payout")
            t1 = t1.find_all('span')
            for i in t1:
                if bool(re.search(r'[0-9]+',i.text)):
                    wide_set_u.append(i.text)
            pay = t2.text.replace(",","").split("円")
            for i in pay:
                if bool(re.search(r'[0-9]+',i)):
                    wide_set_p.append(i)
            race_data["wide_u"] = wide_set_u
            race_data["wide_p"] = wide_set_p
            
            #馬単
            utan_set_u = []
            utan_set_p = []
            utan_data = order.find('tr',class_="Umatan")
            t1 = utan_data.find('td',class_="Result")
            t2 = utan_data.find('td',class_="Payout")
            t1 = t1.find_all('span')
            for i in t1:
                if bool(re.search(r'[0-9]+',i.text)):
                    utan_set_u.append(i.text)
            pay = t2.text.replace(",","").split("円")
            for i in pay:
                if bool(re.search(r'[0-9]+',i)):
                    utan_set_p.append(i)
            race_data["umatan_u"] = utan_set_u
            race_data["umatan_p"] = utan_set_p
            
            #三連複
            fuku3_set_u = []
            fuku3_set_p = []
            huku3_data = order.find('tr',class_="Fuku3")
            t1 = huku3_data.find('td',class_="Result")
            t2 = huku3_data.find('td',class_="Payout")
            t1 = t1.find_all('span')
            for i in t1:
                if bool(re.search(r'[0-9]+',i.text)):
                    fuku3_set_u.append(i.text)
            pay = t2.text.replace(",","").split("円")
            for i in pay:
                if bool(re.search(r'[0-9]+',i)):
                    fuku3_set_p.append(i)
            race_data["sanrenfuku_u"] = fuku3_set_u
            race_data["sanrenfuku_p"] = fuku3_set_p
            #三連単
            tan3_set_u = []
            tan3_set_p = []
            tan3_data = order.find('tr',class_="Tan3")
            t1 = tan3_data.find('td',class_="Result")
            t2 = tan3_data.find('td',class_="Payout")
            t1 = t1.find_all('span')
            for i in t1:
                if bool(re.search(r'[0-9]+',i.text)):
                    tan3_set_u.append(i.text)
            pay = t2.text.replace(",","").split("円")
            for i in pay:
                if bool(re.search(r'[0-9]+',i)):
                    tan3_set_p.append(i)
            race_data["sanrentan_u"] = tan3_set_u
            race_data["sanrentan_p"] = tan3_set_p
            
            agg_data_set.append(json.dumps(race_data))
    agg_file_name = "{}.json".format(race_date)
    agg_data_set = "\n".join(agg_data_set)
    bucket_name_agg = os.getenv('BUCKET_NAME_AGG')
    bucket_agg = client.get_bucket(bucket_name_agg)
    gcs_upload(agg_data_set,agg_file_name,bucket_agg)
    return "Done"
