from itertools import permutations
import os
from google.cloud import storage
from google.cloud import pubsub
import json
from datetime import datetime,timezone,timedelta

def predict_result_output(event, context):
    project_id = os.getenv("GCP_PROJECT")
    client = storage.Client(project_id)
    file_name  = event['name']
    buket_name = event['bucket']
    bucket= client.get_bucket(buket_name)
    blob = bucket.get_blob(file_name)
    JST = timezone(timedelta(hours=+9), 'JST')
    race_date = datetime.now(JST)
    #予測対象レース数
    pred_race_num = 0
    #単勝正解率
    tan_t = 0
    tan_sum = 0
    #連対数
    ren1 = 0
    ren2 = 0
    ren3 = 0
    ren_sum = 0
    fuku3_sum = 0
    for data in blob.download_as_string().decode('utf-8').split('\n'):
        pred_race_num += 1
        race_data = json.loads(data)
        no1 = str(race_data["no1"])
        no2 = str(race_data["no2"])
        no3 = str(race_data["no3"])
        no4 = str(race_data["no4"])
        no5 = str(race_data["no5"])
        no6 = str(race_data["no6"])
        no7 = str(race_data["no7"])
        ren_main = [no1,no2,no3]
        fuku_main = [no1,no2]
        sub_horse = [no1,no2,no3,no4,no5,no6,no7]
        #単勝チェック
        tan_h = race_data["tan_u"]
        tan_p = race_data["tan_p"]
        if no1 in tan_h:
            tan_t += 1
            tan_sum += int(tan_p[tan_h.index(no1)])
        #連帯率の計算
        uren_h = race_data["umaren_u"]
        uren_p = race_data["umaren_p"]
        if no1 in uren_h:
            ren1 += 1
        if no2 in uren_h:
            ren2 += 1
        if no3 in uren_h:
            ren3 += 1
        #馬連の買い目
        bet_set_ren = []
        for z in ren_main:
            for sub in sub_horse:
                if z != sub:
                    bet = sorted([z,sub])
                    bet_set_ren.append('-'.join(bet))
        bet_set_ren = list(set(bet_set_ren))
        #馬連のチェック
        pay_count = 0          
        for i in range(0,len(uren_h),2):
            ren_ture = '-'.join(uren_h[i:i+2])
            if ren_ture in bet_set_ren:
                ren_sum += int(uren_p[pay_count])
            pay_count += 1

        #連帯率の計算
        fuku3_h = race_data["sanrenfuku_u"]
        fuku3_p = race_data["sanrenfuku_p"]
        #三連服の買い目
        bet_set_fuku3 = []
        sub_ren3 = [list(i) for i in list(permutations(sub_horse, 2))]
        for z in fuku_main:
            for sub in sub_ren3:
                if z not in sub:
                    bet = sorted([z,sub[0],sub[1]])
                    bet_set_fuku3.append('-'.join(bet))
        bet_set_fuku3 = list(set(bet_set_fuku3))
        #馬連のチェック
        pay_count = 0          
        for i in range(0,len(fuku3_h),3):
            ren_ture = '-'.join(fuku3_h[i:i+3])
            if ren_ture in bet_set_fuku3:
                fuku3_sum += int(fuku3_p[pay_count])
            pay_count += 1
    bet_sum_tan = pred_race_num*100
    bet_sum_ren = len(bet_set_ren)*100*pred_race_num
    bet_sum_fuku3 = len(bet_set_fuku3)*100*pred_race_num
    
    tan_t = round(((tan_t/pred_race_num)*100),2)
    #連対数
    ren1 = round(((ren1/pred_race_num)*100),2)
    ren2 = round(((ren2/pred_race_num)*100),2)
    ren3 = round(((ren3/pred_race_num)*100),2)
    return_tan = round((tan_sum/bet_sum_tan)*100,2)
    return_ren = round((ren_sum/bet_sum_ren)*100,2)
    return_fuku3 = round((fuku3_sum/bet_sum_fuku3)*100,2)
    tweet_msg = """
    {}::対象レース数:{}R
    1着内率
    ◎:{}%
    連対率
    ◎:{}%　○:{}%　▲:{}%
    
    単勝回収率　　{}%
    馬連回収率　　{}%
    三連複回収率　{}%
    
    """.format(race_date.strftime('%Y/%m/%d'),str(pred_race_num),str(tan_t),str(ren1),str(ren2),str(ren3),str(return_tan),str(return_ren),str(return_fuku3))
    topic_name = os.getenv('TOPIC_NAME')
    publisher = pubsub.PublisherClient()    
    topic_path = publisher.topic_path(project_id, topic_name)
    
    future = publisher.publish(topic_path, data=tweet_msg.encode('utf-8'))
    return "Done"
        