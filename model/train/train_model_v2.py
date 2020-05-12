'''
from itertools import product

import numpy as np

from sklearn import datasets
from sklearn import model_selection
from sklearn.linear_model import LogisticRegression

import mlflow
import mlflow.sklearn

# datasetからwineを取得
X, y = datasets.load_wine(return_X_y=True)

# wine_classifyという名前でmlflowのexperimentを生成
mlflow.set_experiment('wine_classify')
tracking = mlflow.tracking.MlflowClient()
experiment = tracking.get_experiment_by_name('wine_classify')

# logistic regressionで利用するパラメータを用意
solvers = ['newton-cg', 'lbfgs', 'liblinear']
Cs = [1.0, 10.0, 100.0]
# crossvalidateで算出するスコア
scoring = ['precision_macro', 'recall_macro', 'f1_macro']

# solverとCの値を変えつつループ
for solver, c in product(solvers, Cs):
    # experimentのidを指定してstart_run
    with mlflow.start_run(experiment_id=experiment.experiment_id, nested=True):
        # solverとCの値を設定してLogisticRegressionのestimatorを作る
        estimator = LogisticRegression(
            solver=solver, multi_class='auto', max_iter=30000, C=c)
        # cross_validateを使って各種スコアを算出
        scores = model_selection.cross_validate(
            estimator, X, y, n_jobs=-1, cv=3, scoring=scoring,
            return_train_score=True)
        # cvごとのスコアをmeanしてmetricsに設定
        mean_scores = dict([(k, v.mean()) for k, v in scores.items()])
        mlflow.log_metrics(mean_scores)
        # solverとCのパラメータを設定
        mlflow.log_param('solver', solver)
        mlflow.log_param('C', c)
        # 全件でモデル生成して保存
        model = estimator.fit(X, y)
        mlflow.sklearn.log_model(model, 'wine_models')
        print(solver, c, '{:.5f}, {:.5f}, {:5f}'.format(
            mean_scores['test_precision_macro'], mean_scores['test_recall_macro'], mean_scores['test_f1_macro']))
'''
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import lightgbm as lgb
import warnings
from operator import itemgetter
from sklearn.model_selection import StratifiedKFold,KFold,train_test_split
from itertools import product
import tempfile
import os

import mlflow
import mlflow.lightgbm

from sklearn.metrics import accuracy_score,f1_score,roc_curve, roc_auc_score,log_loss

#前処理後のデータを分割
train = pd.read_csv('../EDA/train.csv')
test = pd.read_csv('../EDA/test.csv')
x_columns = train.columns[1:]
x_train = train.iloc[:,1:].values
y_train = train.iloc[:,0]

#交差検証の設定
num_kfold = 5
kf = KFold(n_splits=num_kfold,shuffle=True,random_state=1)

#ハイパーパラメータチューニング対象
boosting_type = ['gbdt','dart']
learning_rate = [0.001,0.01]
num_iterations = [3000,4000,5000]
early_stopping_rounds_weight = [0.7,0.8,0.9]

#Mlflowの設定
#tracking_uri = "../mlruns"
#mlflow.set_tracking_uri(tracking_uri)

experiment_name = 'model_v2 top3 binary2'
mlflow.set_experiment(experiment_name)
tracking = mlflow.tracking.MlflowClient()
experiment = tracking.get_experiment_by_name(experiment_name)

# 実際のトレーニング

num=1
step = 0
train_loss = []
test_loss= []

train_auc= []
test_auc = []

train_f1 = []
test_f1 = []

train_acc = []
test_acc = []
#グリッドサーチ用のループ
for bs_type,eta,iters,esr in product(boosting_type,learning_rate, num_iterations,early_stopping_rounds_weight):
    #交差検証用のループ
    num=num+1
    with mlflow.start_run(experiment_id=experiment.experiment_id, nested=True):
        #mlflow.set_tag("dddd",num)
        #,early_stopping_rounds_weight):
        #学習モデルの基本的な設定
        for i,(train_index, test_index) in enumerate(kf.split(x_train, y_train)):
            params = {
            'boosting_type': bs_type,
            'objective': 'binary',
            'metric': 'binary_logloss',
            'learning_rate':eta,
            'num_iterations':iters,
            'verbose': 0,
            'early_stopping_round': int(esr*iters),
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
            '''
            各モデルの学習推移(loss(train,test))が欲しい(cv分)train中のデータ
            '''
            model = lgb.train(params, train_data, valid_names=evals_name,valid_sets=[eval_data,train_data],evals_result=evals_result,verbose_eval=False)
            for k,v in evals_result.items():
                for met,los in v.items():
                    met_name = k+'_'+met
                    [mlflow.log_metric(met_name,l,i) for i,l in enumerate(los)]
                    step = len(los)
            #model個々のAccutuary
            
            mlflow.lightgbm.log_model(model,"a{}a{}".format(num,i))
            #train/testの予測結果
            train_loss.append(log_loss(y_train[train_index], model.predict(x_train[train_index])))
            test_loss.append(log_loss(y_train[test_index], model.predict(x_train[test_index])))
            train_auc.append(roc_auc_score(y_train[train_index], model.predict(x_train[train_index])))
            test_auc.append(roc_auc_score(y_train[test_index], model.predict(x_train[test_index])))
            sigmoid = lambda x : 1 / (1 + np.exp(-x))
            acc_data_train = [0 if sigmoid(i)>0.5 else 1 for i in model.predict(x_train[train_index])]
            acc_data_test =  [0 if sigmoid(i)>0.5 else 1 for i in model.predict(x_train[test_index])]
            train_f1.append(f1_score(y_train[train_index], acc_data_train))
            test_f1.append(f1_score(y_train[test_index], acc_data_test))
            train_acc.append(accuracy_score(y_train[train_index], acc_data_train))
            test_acc.append(accuracy_score(y_train[test_index], acc_data_test))
            #featuer inportanceの画像を保存
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

        
        mlflow.log_metric("train loss",np.mean(train_loss))
        mlflow.log_metric("train auc",np.mean(train_auc))
        mlflow.log_metric("train f1",np.mean(train_f1))
        mlflow.log_metric("train acc",np.mean(train_acc))
        mlflow.log_metric("test loss",np.mean(test_loss))
        mlflow.log_metric("test auc",np.mean(test_auc))
        mlflow.log_metric("test f1",np.mean(test_f1))
        mlflow.log_metric("test acc",np.mean(test_acc))