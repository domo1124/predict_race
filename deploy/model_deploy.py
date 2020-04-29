import argparse
import glob
import pathlib
import zipfile
import os
import yaml
import json

#Storageにアップロードするモデル名の引数を受け取る
parser = argparse.ArgumentParser(description='Process model functions deploy')
parser.add_argument('model_name', metavar='N', type=str, 
                    help='model name')
args = parser.parse_args()

with open('../config/model_function_deploy.yaml','r') as f:
    conf = yaml.load(f)

with open('../config/gcp.yaml','r') as f:
    gcp = yaml.load(f)

with open("../config/model_functions_args.json") as f:
    function_args_set = json.load(f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=gcp['REDENTIALS_JSON']
project_id = gcp['GCP_PROJECT']
account = gcp['FUNCTIONS_ACCOUNT']
#model配下を確認し、model名と同じディレクトリを確認
deploy_path = '../model/{}'.format(args.model_name)

p_temp = pathlib.Path(deploy_path)
deploy_model={p.name:p for p in p_temp.iterdir() if p.is_dir()}

#特徴量作成のモジュールと予測モジュールをStorageにアップロード
if len(deploy_model) != 0:
    #functions
    if 'functions' in deploy_model:
        deploy_functions = deploy_model["functions"]
        deploy_functions =[ p for p in deploy_functions.iterdir() if p.is_dir()]
        for function in deploy_functions:
            zip_file_name = "{}.zip".format(function.name)
            zip_file_list = [p for p in function.glob('**/*') if 'yaml' not in str(p)]
            with zipfile.ZipFile(zip_file_name, 'w', compression=zipfile.ZIP_DEFLATED) as new_zip:
                for i in zip_file_list:
                    new_zip.write(str(i),arcname=i.name)
            #storageにアップロード
            #blob = bucket.blob(zip_file_name)
            #blob.upload_from_filename(zip_file_name)
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
    else:
        print("Not Found Functions")

    if 'learning_model' in deploy_model:
        deploy_learn_model = deploy_model["learning_model"]
        deploy_learn_model =[ p for p in deploy_learn_model.iterdir() if p.is_dir()]
        for function in deploy_learn_model:
            zip_file_name = "{}.zip".format(function.name)
            zip_file_list = [p for p in function.glob('**/*') if 'yaml' not in str(p)]
            with zipfile.ZipFile(zip_file_name, 'w', compression=zipfile.ZIP_DEFLATED) as new_zip:
                for i in zip_file_list:
                    new_zip.write(str(i),arcname=i.name)
            #storageにアップロード
            #blob = bucket.blob(zip_file_name)
            #blob.upload_from_filename(zip_file_name)
            #アップロード済ファイル削除
            os.remove(zip_file_name)
    else:
        print("Not Found Learning Model")
    
    #learning_model

else:
    print("Not Found Model")
#functionsにアップロードするためのストレージを作成



#学習済みモデルをStorageにアップロード

#StorageにアップロードしたFunctiomをデプロイ
