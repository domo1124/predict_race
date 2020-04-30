from google.cloud import bigquery
from google.cloud import storage
import os

def predict_update(project_name):  
    bgq_client = bigquery.Client()
    stg_client = storage.Client(project_name)
    dataset_name = 'race_data_set'
    buket_name  = 'race_data_set'
    file_name = 'race_result.json' 
    table_name = 'race_result'     
    #race_info
    uri="gs://{}/{}/{}".format(buket_name,table_name,file_name)
    table_ref = bgq_client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    load_job = bgq_client.load_table_from_uri(uri, table_ref, job_config=job_config)  # API request
    print("Starting job {}".format(load_job.job_id))
    load_job.result()
    print("Job finished.")
    return 'Done'



PROJECTID = 'horse-predict-273005'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='/home/h-kobayashi/horse-predict-273005-b1095c86d135.json'
predict_update(PROJECTID)

#Access token :1252142376052641792-RWhPEsCp1bBvmW8MwuegX7ECpXjEhk
#Access token secret :mKpVtCMOH3DviPaTrd4tlQnPJzeHFPaKfLauVgeRm8AD5