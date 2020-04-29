from bs4 import BeautifulSoup
import requests
import json
import base64
import re
import tempfile
from google.cloud import storage
import os
import datetime

def past_transform(co):
    #コーナー順位を変換
    p1=p2=p3=p4 = 0
    try:
        pp=co.split('-')
        if len(pp) != 4:
            if len(pp) == 3:
                p2=pp[0]
                p3=pp[1]
                p4=pp[2]
            elif len(pp) == 2:
                p3=pp[0]
                p4=pp[1]
            elif len(pp) == 1:
                p4=pp[0]
        else:
            p1=pp[0]
            p2=pp[1]
            p3=pp[2]
            p4=pp[3]
    except:
        return p1,p2,p3,p4
    return p1,p2,p3,p4

def weight_transform(weight):
    p_dif_wight = r'\(.+\)'
    try:
        dif_horse_weight = re.search(p_dif_wight,weight).group(0)
        horse_weight = weight.replace(dif_horse_weight,'')
        dif_weight= dif_horse_weight.replace('(','').replace(')','')
    except:
        return None,None
    return horse_weight,dif_weight

def time_transform(time):
    try:
        t_la = time.split(':')
        tl=float(t_la[0])*60
        tl=float(t_la[1])+tl
    except: 
        tl = None

    return tl
def gcs_upload(data,file_name,bucket):
    _, temp_local_filename = tempfile.mkstemp()
    with open(temp_local_filename,'w', encoding = 'utf-8') as f:
        f.write(data)

    blob = bucket.blob(file_name)
    blob.upload_from_filename(temp_local_filename)
    
    os.remove(temp_local_filename)

def past_race_get(event, context):
    race_id = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    #BigQuery更新用のファイル
    result_all = []
    info_all = []
    pay_all =[]
    #バケット名を指定してbucketを取得
    project_name = os.getenv('GCP_PROJECT')#環境変数
    client = storage.Client(project_name)
    '''
    mst_bucket_name = os.getenv('MST_BUCKET')#環境変数
    bucket_race_data= client.get_bucket(mst_bucket_name)
    '''
    #過去レースのHTMLファイルをアップロードするバケット
    bucket_name_html = os.getenv('BUCKET_NAME_RESULT_RACE_HTML')
    bucket_html = client.get_bucket(bucket_name_html)
    #過去レースの結果ファイルをアップロードするバケット
    bucket_name_result = os.getenv('BUCKET_NAME_RESULT_RACE_DATA')
    bucket_result = client.get_bucket(bucket_name_result)
    #過去レース情報の結果ファイルをアップロードするバケット
    bucket_name_info = os.getenv('BUCKET_NAME_RESULT_RACE_INFO')
    bucket_info= client.get_bucket(bucket_name_info)
    #過去レース情報の結果ファイルをアップロードするバケット
    bucket_name_pay = os.getenv('BUCKET_NAME_RESULT_RACE_PAY')
    bucket_pay= client.get_bucket(bucket_name_pay)
    #パースするテーブルクラス名
    race_result_table_class1 = 'race_table_01 nk_tb_common'
    race_result_table_class2 = 'result_table_02'
    return_pay_table_class  = 'pay_table_01'
    #使用する正規表現のパターン
    p_years = r'([0-9]+)'
    p_sex   = r'(牝|牡|セ)'
    p_place = r'(東京|中山|阪神|京都|小倉|新潟|福島|中京|札幌|函館)'
    p_rank1   = r'(新馬|未勝利|500万下|1000万下|1600万下|1勝クラス|2勝クラス|3勝クラス|オープン)'
    p_rank2   = r'(.歳以上|.歳)'
    p_kind   = r'(芝|ダート|ダ)'


    print('{}::start'.format(race_id))
    url="https://db.netkeiba.com/race/{}/".format(race_id)
    rr = requests.get(url)
    html_u = rr.content
    order = BeautifulSoup(html_u, 'html.parser')
    html_file_name = '{}.html'.format(race_id)
    #GCSにアップロード
    gcs_upload(order.prettify(),html_file_name,bucket_html)
    
    #地方競馬のレース結果を除く
    info = order.find('div',class_='data_intro')
    race_info2 = info.find('p',class_="smalltxt").text.split(' ')
    if bool(re.search(p_place,race_info2[1])) == False:
        print('{}::Exclusion'.format(race_id))
    else:
        #レース情報
        
        race_num =  re.search(r'[0-9]+',info.find('dt').text).group(0)
        race_name = info.find('h1').get_text()
        race_info1 = info.find('span').text.split('/')
        date      = race_info2[0]#.replace('年','').replace('月','').replace('日','')
        race_rank = race_info2[2]
        distance  = re.search(r'[0-9]+',race_info1[0]).group() if bool(re.search(r'[0-9]+',race_info1[0])) else None
        kind_info = race_info1[2].split(':')
        kind      = re.search(p_kind,kind_info[0]).group() if bool(re.search(p_kind,kind_info[0])) else None
        condition = kind_info[1].replace('\xa0','').replace(" ","")
        rank1     = re.search(p_rank2,race_rank).group() if bool(re.search(p_rank2,race_rank)) else None
        rank2     = re.search(p_rank1,race_rank).group() if bool(re.search(p_rank1,race_rank)) else None
        place     = re.search(p_place,race_info2[1]).group() 
        #レース結果
        race_result_json = {} 
        race_data_list=[]
        result_table = order.find_all("table",class_=race_result_table_class1)
        for i in result_table:
            trs = i.find_all("tr")
            data = trs[1:]#ヘッダー以外のデータ
            horse_count = len(data) 
            for tr in data:
                race_json_dict = {}
                result = list(tr.find_all('td'))
                tyaku = result[0].string
                wakuban= result[1].string
                umaban = result[2].string 
                horse_id = result[3].find("a").get("href").replace('/','').replace('horse','')
                horse_name = result[3].find("a").get("title")
                horse_sex = re.search(p_sex,result[4].string).group(0)
                horse_year = re.search(p_years,result[4].string).group(0)
                kinryo = result[5].string
                jockey = result[6].find("a").get("title")
                time = time_transform(result[7].string)
                p1,p2,p3,p4 = past_transform(result[10].string)
                last3f = result[11].string
                odds = result[12].string
                populer = result[13].string
                horse_weight,dif_horse_weight = weight_transform(result[14].string)
                race_json_dict["race_id"]          = race_id
                race_json_dict["tyaku"]            = int(tyaku) if tyaku.isdecimal() else 99
                race_json_dict["wakuban"]          = int(wakuban)
                race_json_dict["umaban"]           = int(umaban)
                race_json_dict["horse_id"]         = int(horse_id)
                race_json_dict["horse_name"]       = horse_name
                race_json_dict["horse_sex"]        = horse_sex
                race_json_dict["horse_year"]       = int(horse_year)
                race_json_dict["jockey"]           = jockey
                race_json_dict["kinryo"]           = float(kinryo)
                race_json_dict["odds"]             = float(odds) if tyaku.isdecimal() else 0
                race_json_dict["popular"]          = int(populer) if tyaku.isdecimal() else 0
                race_json_dict["time"]             = time
                #race_json_dict["dif_time"]         = dif_time
                race_json_dict["last3f"]           = float(last3f) if tyaku.isdecimal() else 0
                race_json_dict["p1"]               = int(p1)
                race_json_dict["p2"]               = int(p2)
                race_json_dict["p3"]               = int(p3)
                race_json_dict["p4"]               = int(p4)
                race_json_dict["horse_weight"]     = int(horse_weight)  if tyaku.isdecimal() else None
                race_json_dict["dif_horse_weight"] = int(dif_horse_weight) if tyaku.isdecimal() else None
                race_data_list.append(json.dumps(race_json_dict, ensure_ascii=False))
                result_all.append(json.dumps(race_json_dict, ensure_ascii=False))
        result_file_name = "{}.json".format(race_id)
        race_data_list = "\n".join(race_data_list)
        gcs_upload(race_data_list,result_file_name,bucket_result)


        #コーナー通過順位
        conner_set=[]
        conner_table = order.find_all("table",summary="コーナー通過順位")
        for past in conner_table:
            trs = past.find_all("tr")
            for tr in trs:
                conner_set.append(tr.find("td").string)
            if len(conner_set) == 4:
                conner1= conner_set[0]
                conner2= conner_set[1]
                conner3= conner_set[2]
                conner4= conner_set[3]
            elif len(conner_set) == 3:
                conner1=None
                conner2=conner_set[0]
                conner3=conner_set[1]
                conner4=conner_set[2]
            elif len(conner_set) == 2:
                conner1=None
                conner2=None
                conner3=conner_set[0]
                conner4=conner_set[1]
            elif len(conner_set) == 1:
                conner1=None
                conner2=None
                conner3=None
                conner4=conner_set[0]
            else:
                conner1=None
                conner2=None
                conner3=None
                conner4=None

        #ラップタイム
        lap_table = order.find_all("table",summary="ラップタイム")
        for lap in lap_table:
            trs = lap.find_all("tr")
            lap_time = trs[0].find('td').get_text()

        #race_infoのjson作成
        info_json_dict={}
        info_json_list=[]
        info_json_dict["race_id"]     = race_id
        info_json_dict["race_num"]    = int(race_num)
        info_json_dict["race_name"]   = race_name
        info_json_dict["race_date"]   = datetime.datetime.strptime(date,'%Y年%m月%d日').strftime('%Y-%m-%d')
        info_json_dict["race_rank1"]  = rank1
        info_json_dict["race_rank2"]  = rank2
        info_json_dict["distance"]    = int(distance) 
        info_json_dict["kind"]        = kind
        info_json_dict["condition"]   = condition     
        info_json_dict["place"]       = place
        info_json_dict["horse_count"] = horse_count
        info_json_dict["lap_time"]    = lap_time
        info_json_dict["conner1"]     = conner1
        info_json_dict["conner2"]     = conner2
        info_json_dict["conner3"]     = conner3
        info_json_dict["conner4"]     = conner4
        info_all.append(json.dumps(info_json_dict, ensure_ascii=False))

        info_file_name = "{}.json".format(race_id)
        gcs_upload(json.dumps(info_json_dict, ensure_ascii=False),info_file_name,bucket_info)
        #払い戻し
        pay_data_list = []
        p_num = r'([0-9]+)'
        pay_table = order.find_all("table",class_=return_pay_table_class)
        for pay in pay_table:
            trs = pay.find_all("tr")
            for tr in trs:
                get_class = tr.find('th').get('class')
                pay_data  = tr.find_all('td')
                indicator = get_class[0]
                e_horse = pay_data[0].childGenerator()
                e_pay   = pay_data[1].childGenerator()
                for h,p in zip(e_horse,e_pay):
                    horse_set = re.findall(p_num,str(h))
                    if horse_set:
                        pay_json_dict = {}
                        horse_1   = horse_set[0]
                        horse_2   = horse_set[1] if len(horse_set) >=2 else None 
                        horse_3   = horse_set[2] if len(horse_set) >=3 else None
                        pay       = p
                        pay_json_dict["race_id"]   = race_id
                        pay_json_dict["indicator"] = indicator
                        pay_json_dict["horse_1"]   = horse_1
                        pay_json_dict["horse_2"]   = horse_2
                        pay_json_dict["horse_3"]   = horse_3
                        pay_json_dict["pay"]       = pay
                        pay_data_list.append(json.dumps(pay_json_dict, ensure_ascii=False))
                        pay_all.append(json.dumps(pay_json_dict, ensure_ascii=False))
                            

        pay_file_name = "{}.json".format(race_id)
        pay_data_set = "\n".join(pay_data_list)
        gcs_upload(pay_data_set,pay_file_name,bucket_pay)
        '''
        #BigQuery更新用のファイルをGCSにアップロード
        up_day = datetime.datetime.today().strftime("%Y%m%d")
        #race_result更新用ファイルのアップロード
        result_file_all = "race_result/result_{}.json".format(up_day)
        result_all = "\n".join(result_all)
        gcs_upload(result_all,result_file_all,bucket_race_data)
        #race_info更新用ファイルのアップロード
        info_file_all = "race_info/info_{}.json".format(up_day)
        info_all = "\n".join(info_all)
        gcs_upload(info_all,info_file_all,bucket_race_data)
        #race_pay更新用ファイルのアップロード
        pay_file_all = "race_pay/pay_{}.json".format(up_day)
        pay_all = "\n".join(pay_all)
        gcs_upload(pay_all,pay_file_all,bucket_race_data)
        '''

    return 'Done'  