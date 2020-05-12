import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import lightgbm as lgb

from operator import itemgetter
from sklearn.model_selection import StratifiedKFold,KFold,train_test_split
from itertools import product
import os

import mlflow
import mlflow.lightgbm

from sklearn.metrics import accuracy_score,f1_score,roc_curve, roc_auc_score,log_loss
import argparse


#の引数を受け取る
parser = argparse.ArgumentParser(description='Process model train')
parser.add_argument('exp_name', metavar='N', type=str, 
                    help='experiment_name')
parser.add_argument('--train' ,metavar='N', type=str, 
                    help='train_file_path(csv)')
parser.add_argument('--kfold',type=int, default=5,
                    help='k_fold count')

args = parser.parse_args()
#ファイルの読み込み
train = pd.read_csv(args.train)
x_train = train.iloc[:,:-1].values
y_train = train.iloc[:,-1]
x_columns = train.columns[:-1]
#mlflowの設定
experiment_name = args.exp_name
mlflow.set_experiment(experiment_name)
tracking = mlflow.tracking.MlflowClient()
experiment = tracking.get_experiment_by_name(experiment_name)
#交差検証の設定
num_kfold = args.kfold
kf = KFold(n_splits=num_kfold,shuffle=True,random_state=1)
    
#ハイパーパラメータチューニング対象
boosting_type = ['gbdt','dart']
learning_rate = [0.001,0.01]
early_stopping_rounds_weight = [0.1,0.2,0.3,0.4]
max_depth = [-1,6,7,8]

#グリッドサーチするハイパーパラメータの全通りの組み合わせをループ
for bs_type,eta,depth,esr in product(boosting_type,learning_rate, max_depth,early_stopping_rounds_weight):
    #交差検証用のループ
    step = 0
    #loss
    train_loss = []
    eval_loss  = []
    #auc
    train_auc = []
    eval_auc  = []
    with mlflow.start_run(experiment_id=experiment.experiment_id, nested=True):
        #学習モデルの基本的な設定
        for i,(train_index, test_index) in enumerate(kf.split(x_train, y_train)):
            print("{}_cv{} start".format(experiment_name,i))
            params = {
            'boosting_type': bs_type,
            'objective': 'binary',
            'metric': 'binary_logloss',
            'learning_rate':eta,
            'num_iterations':10000,
            'max_depth': depth,
            'early_stopping_round': int(esr*10000),
            }
            #ハイパーパラメータをMlflowに記載
            #lightgbmの学習遷移を格納する配列
            evals_result={}
            #学習遷移のdictのKey
            evals_name=["eval_cv{}".format(i),"train_cv{}".format(i)]
            mlflow.log_params(params)
            #LightGBM用のデータに変換
            train_data = lgb.Dataset(x_train[train_index], label=y_train[train_index])
            eval_data  = lgb.Dataset(x_train[test_index], label=y_train[test_index], reference= train_data)
            #モデルの学習    学習モデルの保存MLflow
            model = lgb.train(params, train_data, valid_names=evals_name,valid_sets=[eval_data,train_data],evals_result=evals_result,verbose_eval=False)
            for k,v in evals_result.items():
                for met,los in v.items():
                    met_name = k+'_'+met
                    [mlflow.log_metric(met_name,l,i) for i,l in enumerate(los)]
                    step = len(los)
            #モデルの保存
            mlflow.lightgbm.log_model(model,"{}_cv{}".format(experiment_name,i))
            #train/testのloss
            train_loss.append(log_loss(y_train[train_index], model.predict(x_train[train_index])))
            eval_loss.append(log_loss(y_train[test_index], model.predict(x_train[test_index])))
            #train/evalのauc
            train_auc.append(roc_auc_score(y_train[train_index], model.predict(x_train[train_index])))
            eval_auc.append(roc_auc_score(y_train[test_index], model.predict(x_train[test_index])))
            #featuer_importanceのグラフ作成
            feature_imp = pd.DataFrame(sorted(zip(model.feature_importance(),x_columns)), columns=['Value','Feature'])
            plt.figure()
            sns.barplot(x="Value", y="Feature", data=feature_imp.sort_values(by="Value", ascending=False))
            plt.title('LightGBM Features')
            plt.tight_layout()
            file_name = './importance{}.png'.format(num)
            plt.savefig(file_name)
            mlflow.log_artifact(file_name)
            plt.close()
            os.remove(file_name)
            #AUC曲線のグラフ作成
            fpr, tpr, thresholds = roc_curve(y_train[test_index], model.predict(x_train[test_index]))
            plt.figure()
            plt.plot(fpr, tpr, label='SVC (AUC = %.2f)'%eval_auc[-1])
            plt.legend()
            plt.xlim([0, 1])
            plt.ylim([0, 1])
            plt.title('ROC curve')
            plt.xlabel('False Positive Rate')
            plt.ylabel('True Positive Rate')
            plt.grid(True)
            file_name = './ROC{}.png'.format(num)
            plt.savefig(file_name)
            mlflow.log_artifact(file_name)
            plt.close()
            os.remove(file_name)
        #交差検証の平均      
        mlflow.log_metric("train loss",np.mean(train_loss))
        mlflow.log_metric("eval loss",np.mean(train_loss))

        mlflow.log_metric("train auc",np.mean(train_auc))
        mlflow.log_metric("eval acc",np.mean(eval_acc))