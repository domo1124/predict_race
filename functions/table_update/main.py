from google.cloud import bigquery
from google.cloud import storage
import os

def predict_update(event, context):
    file_name  = event['name']
    buket_name = event['bucket']
    print(file_name)
    project_name = os.getenv('GCP_PROJECT')    
    bgq_client = bigquery.Client()
    stg_client = storage.Client(project_name)
    dataset_name = 'race_data_set'
    if 'result' in file_name:
        table_name = 'race_result'
        m_buket_name = 'race_result'
    elif 'info' in file_name:
        table_name = 'race_info'
        m_buket_name = 'race_info'
    elif 'pay' in file_name:
        table_name = 'race_pay'
        m_buket_name = 'race_pay'
    else:
        return 'file Faild'        

    uri="gs://{}/{}".format(buket_name,file_name)
    table_ref = bgq_client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    load_job = bgq_client.load_table_from_uri(uri, table_ref, job_config=job_config)  # API request
    print("Starting job {}".format(load_job.job_id))
    load_job.result()
    print("Job finished.")
    return 'Done'