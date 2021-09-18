import time
import re
from bs4 import BeautifulSoup
import datetime
import argparse
import requests
import json
import csv
import os
import cchardet

def update_file(file_name,update_file):
    with open(file_name,'a') as f:
        with open (update_file,'r') as f1:
            f.write(f1.read())
    with open(file_name,'r') as f:
        datalist = f.read().split("\n")
        data = list(dict.fromkeys(datalist))
    with open(file_name,'w') as f:
        d = "\n".join(data)
        f.write(d)
    os.remove(update_file)
            


def output(file_type,result_all,info_all,pay_all):
    f_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    output_file_name_result = 'result_{}.{}'.format(f_time,file_type)
    output_file_name_info = 'info_{}.{}'.format(f_time,file_type)
    output_file_name_odds = 'odds_{}.{}'.format(f_time,file_type)
    #csv/tsv形式で出力
    if file_type == 'csv' or file_type == 'tsv':
        delimiter_str = ',' if file_type == 'csv'  else '\t'

        with open(r'./{}'.format(output_file_name_result),'w',encoding='utf-8',newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = ['race_id', 'tyaku', 'wakuban', 'umaban', 'horse_id', 'horse_name', 'horse_sex', 'horse_year', 'jockey', 'kinryo', 'odds', 'popular', 'time', 'last3f', 'p1', 'p2', 'p3', 'p4', 'horse_weight', 'diff_horse_weight'], delimiter=delimiter_str)
            writer.writeheader()
            writer.writerows(result_all)

        with open(r'./{}'.format(output_file_name_info),'w',encoding='utf-8',newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames =['race_id', 'race_num', 'race_name', 'race_date', 'race_rank1', 'race_rank2', 'distance', 'kind', 'condition', 'place', 'horse_count', 'lap_time', 'conner1', 'conner2', 'conner3', 'conner4'], delimiter=delimiter_str)
            writer.writeheader()
            writer.writerows(info_all)

        with open(r'./{}'.format(output_file_name_odds),'w',encoding='utf-8',newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = ['race_id', 'indicator', 'horse_1', 'horse_2', 'horse_3', 'pay'], delimiter=delimiter_str)
            writer.writeheader()
            writer.writerows(pay_all)

    #BQテーブルにINSERTするためのjson形式で出力
    elif file_type== 'json':
        race_data_list=[json.dumps(i, ensure_ascii=False) for i in result_all]
        result_all = "\n".join(race_data_list)+"\n"
        with open(output_file_name_result,'w',encoding= 'utf-8') as fr:
            fr.write(result_all)

        info_data_list=[json.dumps(i, ensure_ascii=False) for i in info_all] 
        info_all = "\n".join(info_data_list)+"\n"
        with open(output_file_name_info,'w',encoding= 'utf-8') as fr:
            fr.write(info_all)

        pay_data_list=[json.dumps(i, ensure_ascii=False) for i in pay_all]
        pay_all = "\n".join(pay_data_list)+"\n"
        with open(output_file_name_odds,'w',encoding= 'utf-8') as fr:
            fr.write(pay_all)
    return output_file_name_result,output_file_name_info,output_file_name_odds


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

def race_get(race_id,order):
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

    info = order.find('div',class_='data_intro')
    race_info2 = info.find('p',class_="smalltxt").text
    race_num =  re.search(r'[0-9]+',info.find('dt').text).group(0)
    race_name = info.find('h1').get_text().replace("\n","").replace(' ','')
    race_info1 = info.find('span').text.split('/')
    date      = re.search(r'[0-9]+年[0-9]+月[0-9]+日',race_info2).group()#.replace('年','').replace('月','').replace('日','')
    distance  = re.search(r'[0-9]+',race_info1[0]).group() if bool(re.search(r'[0-9]+',race_info1[0])) else None
    kind_info = race_info1[2].split(':')
    kind      =kind_info[0].replace('\xa0','').replace(" ","")
    condition =kind_info[1].replace('\xa0','').replace(" ","")
    rank1     = re.search(p_rank2,race_info2).group() if bool(re.search(p_rank2,race_info2)) else None
    rank2     = re.search(p_rank1,race_info2).group() if bool(re.search(p_rank1,race_info2)) else None
    place     = re.search(p_place,race_info2).group() 
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
            tyaku = result[0].text
            wakuban= result[1].text
            umaban = result[2].text 
            horse_id = result[3].find("a").get("href").replace('/','').replace('horse','')
            horse_name = result[3].find("a").get("title")
            horse_sex = re.search(p_sex,result[4].text).group(0)
            horse_year = re.search(p_years,result[4].text).group(0)
            kinryo = result[5].text
            jockey = result[6].find("a").get("title")
            time = time_transform(result[7].text)
            p1,p2,p3,p4 = past_transform(result[10].text)
            last3f = result[11].text
            last3f = last3f if last3f != '' else 0
            odds = result[12].text
            populer = result[13].text
            horse_weight,dif_horse_weight = weight_transform(result[14].text)
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
            race_json_dict["last3f"]           = float(last3f) if tyaku.isdecimal() else 0
            race_json_dict["p1"]               = int(p1) 
            race_json_dict["p2"]               = int(p2)
            race_json_dict["p3"]               = int(p3)
            race_json_dict["p4"]               = int(p4)
            race_json_dict["horse_weight"]     = int(horse_weight)  if tyaku.isdecimal() else None
            race_json_dict["diff_horse_weight"] = int(dif_horse_weight) if tyaku.isdecimal() else None
            race_data_list.append(race_json_dict)

    #コーナー通過順位
    conner_set=[]
    conner_table = order.find_all("table",summary="コーナー通過順位")
    for past in conner_table:
        trs = past.find_all("tr")
        for tr in trs:
            conner_set.append(tr.find("td").text)
        if len(conner_set) == 4:
            conner1= conner_set[0]
            conner2= conner_set[1]
            conner4= conner_set[3]
            conner3= conner_set[2]
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
        if len(trs) != 0:
            lap_time = trs[0].find('td').get_text().replace("\n","").replace(' ','')
        else:
            lap_time = None

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

    info_json_list.append(info_json_dict)   

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
                    pay       = str(p.string)
                    pay = pay.replace("\n","").replace(' ','').replace(',','')
                    pay_json_dict["race_id"]   = race_id
                    pay_json_dict["indicator"] = indicator
                    pay_json_dict["horse_1"]   = horse_1
                    pay_json_dict["horse_2"]   = horse_2
                    pay_json_dict["horse_3"]   = horse_3
                    pay_json_dict["pay"]       = int(pay)
                    pay_data_list.append(pay_json_dict)
 
    return race_data_list,info_json_list,pay_data_list 


if __name__ == '__main__':
    race_result = []
    race_info = []
    race_odds = []
    #Storageにアップロードするモデル名の引数を受け取る
    parser = argparse.ArgumentParser(description='netkeibaから過去レースのデータを取得する。引数で期間指定する。引数の日付はyyyy-mm-dd形式')
    parser.add_argument('-s', '--start_date', required=True, help="レース結果の取得開始日")
    parser.add_argument('-e', '--end_date', required=True, help="レース結果の取得終了日")
    parser.add_argument('-o', '--file_type', default='csv', help="出力ファイルの形式(csv,tsv,json【json改行区切り(BQ用)】)",choices=['csv', 'tsv','json'])
    parser.add_argument('-u', '--update_type', default='new', help="出力ファイルの更新方法(new,update)。updateの場合は実行pathにracd_result.[csv,tsv,json]、race_info.[csv,tsv,json]、race_odds.[csv,tsv,json]のファイルがあること",choices=['new', 'update'])
    args = parser.parse_args()
    start = datetime.datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(args.end_date, "%Y-%m-%d")

    #取得期間の設定
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days+1)]
    #race_listへアクセス
    for date in date_generated:
    #pageへアクセス
    #1件も無い場合はskip
        race_list_url = "https://db.netkeiba.com/race/list/{}/".format(date.strftime("%Y%m%d"))
        rr = requests.get(race_list_url)
        html_u = rr.content
        bs_data = BeautifulSoup(html_u, 'html.parser')
        #データがある場合は、レース結果ページにデータを取りに行く
        race_list2 = bs_data.find_all('a', href=re.compile('/race/[0-9]+/'))
        if len(race_list2) == 0:
            continue
        else:
            for i in race_list2:
                race_id = i.get('href').replace("race","").replace("/","")
                result_url="https://db.netkeiba.com{}".format(i.get('href'))
                html_result = requests.get(result_url)
                bs = BeautifulSoup(html_result.content.decode("euc-jp", "ignore"), 'html.parser')
                result,info,odds = race_get(race_id,bs)
                race_result += result
                race_info += info
                race_odds += odds
                break

    result_f,info_f,odds_f = output(args.file_type.lower(),race_result,race_info,race_odds)
    if args.update_type == 'update':
        update_file('./result.{}'.format(args.file_type.lower()),result_f)
        update_file('./info.{}'.format(args.file_type.lower()),info_f)
        update_file('./odds.{}'.format(args.file_type.lower()),odds_f)
        

