import os
from datetime import datetime
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from io import BytesIO


def featuer_make(event, context):
    project_id = os.getenv("GCP_PROJECT")
    bucket_name = os.getenv("FEATURE_BUCKET_NAME")
    #取得した出走表データの読み込み
    client = storage.Client(project_id)
    file_name  = event['name']
    buket_name = event['bucket']
    bucket= client.get_bucket(buket_name)
    blob = bucket.get_blob(file_name)
    up_data = blob.download_as_string()
    order_data = pd.read_csv(BytesIO(up_data),dtype={"horse_id": int})
    order_data["race_date"]=pd.to_datetime(order_data["race_date"],format='%Y-%m-%d').dt.date
    '''
    前処理を記載する。
    '''

    #modelに流せる状態でCSV出力
    client = storage.Client()
    # バケットオブジェクト取得
    bucket = client.get_bucket(bucket_name)
    # 保存先フォルダとファイル名作成
    file_name = "feature_{}.csv".format(datetime.now().strftime('%Y%m%d%H%M%S'))
    blob = bucket.blob(file_name)
    blob.upload_from_string(data=mer.to_csv(sep=",", index=False), content_type='text/csv')

    return 'Done'
