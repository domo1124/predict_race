import argparse
import glob
import pathlib
import zipfile
import os
import yaml
import json
from google.cloud import storage
from time import sleep
import subprocess
#Storageにアップロードするモデル名の引数を受け取る
parser = argparse.ArgumentParser(description='Process model functions deploy')
parser.add_argument('model_name', metavar='N', type=str, 
                    help='model name')
args = parser.parse_args()


with open('../config/gcp.yaml','r') as f:
    gcp = yaml.load(f, Loader=yaml.SafeLoader)

with open("../config/model_functions_args.json") as f:
    function_args_set = json.load(f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=gcp['CREDENTIALS_JSON']
project_id = gcp['GCP_PROJECT']
location = gcp['LOCATION']
shell = gcp["SHELL"]
#account = gcp['FUNCTIONS_ACCOUNT']
#model配下を確認し、model名と同じディレクトリを確認
deploy_path = '../model/{}'.format(args.model_name)

p_temp = pathlib.Path(deploy_path)
deploy_model={p.name:p for p in p_temp.iterdir() if p.is_dir()}


storage_clent = storage.Client(project_id)

#特徴量作成のモジュールと予測モジュールをStorageにアップロード
if len(deploy_model) != 0:

    #model Deploy用のバケット作成
    #bucket check
    bucket_check = storage_clent.bucket(args.model_name)
    if bucket_check.exists() == False:
        bucket = storage_clent.create_bucket(args.model_name,location=location)
        sleep(2)
        print("Bucket {} created".format(bucket.name))
    else:
        bucket = storage_clent.get_bucket(args.model_name)
        print("already own this bucket")
    #functions
    if 'functions' in deploy_model:
        deploy_functions = deploy_model["functions"]
        deploy_functions =[ p for p in deploy_functions.iterdir() if p.is_dir()]
        for function in deploy_functions:
            gcloud_cmmand = ["gcloud","functions","deploy","--allow-unauthenticated"]  
            zip_file_name = "{}.zip".format(function.name)
            zip_file_list = [p for p in function.glob('**/*') if 'yaml' not in str(p)]
            with zipfile.ZipFile(zip_file_name, 'w', compression=zipfile.ZIP_DEFLATED) as new_zip:
                for i in zip_file_list:
                    new_zip.write(str(i),arcname=i.name)
            #storageにアップロード
            blob = bucket.blob('functions/{}'.format(zip_file_name))
            blob.upload_from_filename(zip_file_name)
            #アップロード済ファイル削除
            os.remove(zip_file_name)
            gsutil_link = "gs://{}/functions/{}".format(args.model_name,zip_file_name)
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
            #gcloud_cmmand.append("--service-account={}".format(account))
            print(gcloud_cmmand)
            proc = subprocess.run(gcloud_cmmand,shell =shell)
            print("{} Deploy End".format(function.name))
            #allUsers権限を削除
            delete_allunsers_command = ["gcloud","functions","remove-iam-policy-binding",function.name, "--member=allUsers","--role=roles/cloudfunctions.invoker"] 
            proc = subprocess.run(delete_allunsers_command,shell =shell)
            print("{} allUsers Delete".format(function.name))  
    else:
        print("Not Found Functions")

    if 'learning_model' in deploy_model:
        deploy_learn_model = deploy_model["learning_model"]
        deploy_learn_model =[ p for p in deploy_learn_model.iterdir() if p.is_dir()]
        for model in deploy_learn_model:
            for file in model.glob('**/*'):
                blob = bucket.blob('model/{}/{}'.format(model.name,file.name))
                blob.upload_from_filename(file)
                sleep(1)
            print("{} pickle upload".format(model.name))

    else:
        print("Not Found Learning Model")


else:
    print("Not Found Model")

