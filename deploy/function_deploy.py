import shutil
import pathlib
import glob
import re
import zipfile
import os
from google.cloud import storage
import yaml
import json
import subprocess
#yamlファイルからGCPを使うための設定を取得
with open('../config/function_deploy.yaml','r') as f:
    conf = yaml.load(f)

with open('../config/gcp.yaml','r') as f:
    gcp = yaml.load(f)

with open("../config/functions_args.json") as f:
    function_args_set = json.load(f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=gcp['REDENTIALS_JSON']
project_id = gcp['GCP_PROJECT']
account = gcp['FUNCTIONS_ACCOUNT']

p_temp = pathlib.Path('../functions')
function_list=[p for p in p_temp.iterdir() if p.is_dir()]

client = storage.Client(project_id)
bucket_name = conf['FUNCTION_BUCKET']

bucket = client.get_bucket(bucket_name)
deploy_func_set = []

for deploy_func in function_args_set:
    deploy_func_set.append(list(deploy_func.keys())[0])

for function in function_list:
    if function.name in deploy_func:
        print("Not Deploy {} ".format(function.name))
    else:
        gcloud_cmmand = ["gcloud","functions","deploy","--allow-unauthenticated"]   
        zip_file_name = "{}.zip".format(function.name)
        zip_file_list = [p for p in function.glob('**/*') if 'yaml' not in str(p)]
        with zipfile.ZipFile(zip_file_name, 'w', compression=zipfile.ZIP_DEFLATED) as new_zip:
            for i in zip_file_list:
                new_zip.write(str(i),arcname=i.name)
        #storageにアップロード
        blob = bucket.blob(zip_file_name)
        blob.upload_from_filename(zip_file_name)
        #アップロード済ファイル削除
        os.remove(zip_file_name)
        gsutil_link = "gs://{}/{}".format(bucket_name,zip_file_name)
        for arg_f in function_args_set:
            #argsファイル内に存在していた場合、コマンドを設定
            
            if function.name in arg_f:
                arg_data = arg_f[function.name]
                runtime = arg_data['runtime']
                trigger= arg_data['trigger']
                if type(trigger) is dict:
                    trigger_type = list(trigger.keys())[0]
                    trigger_event = list(trigger.values())[0]
                    trigger_command = "--trigger-{}={}".format(trigger_type,trigger_event)
                else:
                    trigger_type = trigger
                    trigger_command = "--trigger-{}".format(trigger_type)
                memory= arg_data['memory']
                timeout= arg_data['timeout']
                entry_point= arg_data['entry_point']

        env_file = list(function.glob('**/*.yaml'))
        if len(env_file) != 0:
            env_file = env_file[0]
        #コマンドの設定
        print("{} Deploy Start".format(function.name))
        gcloud_cmmand.append(function.name)
        gcloud_cmmand.append("--runtime={}".format(runtime))
        gcloud_cmmand.append(trigger_command)
        gcloud_cmmand.append("--timeout={}".format(timeout))
        gcloud_cmmand.append("--memory={}".format(memory))
        gcloud_cmmand.append("--source={}".format(gsutil_link))
        gcloud_cmmand.append("--entry-point={}".format(entry_point))
        gcloud_cmmand.append("--env-vars-file={}".format(env_file))
        gcloud_cmmand.append("--service-account={}".format(account))
        proc = subprocess.run(gcloud_cmmand,shell =True)
        print("{} Deploy End".format(function.name))
        
