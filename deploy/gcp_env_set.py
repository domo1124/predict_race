from google.cloud import storage
from google.cloud import bigquery
from google.cloud import pubsub
import json
import yaml
import os
from time import sleep
#GCPを使うための設定
#yamlファイルからGCPを使うための設定を取得
print("GCP Storage Pubsub Topic BigQuery Dataset and Table make start")

with open('../config/gcp.yaml','r') as f:
    gcp = yaml.load(f, Loader=yaml.SafeLoader)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=gcp['CREDENTIALS_JSON']
project_id = gcp['GCP_PROJECT']
location = gcp['LOCATION']

with open('../config/gcp_env_set.yaml','r') as f:
    conf = yaml.load(f, Loader=yaml.SafeLoader)



storage_list = conf['BUCKET_LIST']
topic_list = conf['TOPIC_LIST']
data_set_list = conf['BIGQUERY_DATASET_LIST']
bigquery_table_list = conf['BIGQUERY_TABLE_LIST']
#Storageの作成
storage_clent = storage.Client(project_id)
for bucket in storage_list:
    bu = storage_clent.create_bucket(bucket,location=location)
    sleep(2)
    print("Bucket {} created".format(bu.name))

#Pub/Sub Topicの作成
pubsub_clent = pubsub.PublisherClient()
for topic in topic_list:
    topic_path = pubsub_clent.topic_path(project_id,topic)
    res = pubsub_clent.create_topic(topic_path)
    print("Topic created: {}".format(topic))

#BigQuery Datasetの作成
client_bigquery = bigquery.Client(project_id)
for dataset in data_set_list:
    ds = bigquery.Dataset("{}.{}".format(project_id,dataset))
    ds.location = location
    ds = client_bigquery.create_dataset(ds)
    sleep(2)
    print("Created dataset {}.{}".format(client_bigquery.project, ds.dataset_id))

#BigQuery Tableの作成
for table_data in bigquery_table_list:
    dataset_id = table_data["dataset"]
    dataset_ref = client_bigquery.dataset(dataset_id)
    table_id = table_data["table"]
    schema_file_path = table_data["schema_json_path"]
    with open(schema_file_path,'r') as f:
        schema_data = json.load(f)
    schema = [bigquery.SchemaField(sch["name"], sch["type"],mode=sch["mode"]) for sch in schema_data]

    table = bigquery.Table(dataset_ref.table(table_id), schema=schema)
    external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
    external_config.source_uris = table_data["source_uri"]
    table.external_data_configuration = external_config
    table = client_bigquery.create_table(table)
    sleep(2)
    print("Created table {}".format(table_id))