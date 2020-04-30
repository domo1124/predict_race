# 競馬予想AI　Twitter Bot

## Overview  
1.データ収集  
 レース予測に必要なデータをwebサイトから収集する.  
 収集したデータをGCP Storageにアップロードする.  
 
2.予測  
 収集したデータを元に特徴量を作成し、GCP Storageにアップロードされている学習済みモデルを読み込んで予測する.  
 予測の流れは、featuer_makeとpredictを分けてモジュールを実行する.  
 予測した結果をtwitterにアップする.  


## GCP Functions 

 #### 1.week_race_get (node.js 8) 
 * 開催されるレースの一覧を取得する.  
 * レース一覧をスクレイピングするサイトでjavascriptを実行する必要があるため、  
  GCP Functionsが対応しているnodejsで動くHeadless Chromeのpuppeteerを使用.    
 * GCP Pub/Subで取得したレース一覧をtopicにpublishする.  
 __Source code:[week_race_get](/functions/week_race_get/index.js)__ 
 
 #### 2.predict_race (python 3.7) 
 * 出走頭数、レースの出走距離等の条件で予測対象のレースを絞り込みを行う.  
 * 予測対象に選ばれたレース一覧をjson形式でまとめて、GCP Storageにアップロードする. 
 * 予測対象レースに出走する競走馬のURL一覧を、GCP Pub/Subでtopicにpublishする.  
  __Source code:[predict_race](/functions/predict_race/main.py)__ 
  
 #### 3.predict_horse (python 3.7) 
 * レースに出走する競走馬の過去に出走したレース結果を取得する. 
 * Big Queryの外部テーブルの参照先のGCP Storageにjsonファイルがないレースのみ抽出する. 
 * jsonファイルが存在しないレースのURLを、GCP Pub/Subでtopicにpublishする.  
  __Source code:[predict_horse](/functions/predict_horse/main.py)__ 
  
 #### 4.past_race_get (python 3.7) 
 * jsonファイルの存在しないURLに接続して、レースデータのjsonファイルを作成する。
 * 作成するファイルは以下の通り.  
   - race_info.json : レースの情報 
   - race_result.json :　レースの結果
   - race_pay.json : レースの配当金
  __Source code:[past_race_get](/functions/past_race_get/main.py)__  
  
 #### 5.predict_race_check (python 3.7) 
 * GCP Storage上にアップロードされている予測対象レースファイルを参照し、  
 開催時間の1時間前かどうか判断する.
 * 1時間前ならレースに出走する競走馬一覧のCSVファイルを作成し、GCP Storageにアップロードする.  
  __Source code:[predict_race_check](/functions/predict_race_check/main.py)__  
  
 #### 6.featuer_make (python 3.7) 
 * 競走馬一覧CSVファイルから予測モデルに流す特徴量を作成.  
 * 前処理済の特徴量のCSVファイルをGCP Storageにアップロードする.   
  __Source code:[AboutMe](/AboutMe.md)__  
  
 #### 7.predict_model (python 3.7) 
 * GCP Storageにある前処理済の特徴量のCSVファイルを読み込み予測を行う.  
 * 予測結果をjsonファイルに出力しGCP Storageにアップロードする.  
 * 予測結果のフォーマットを整えて、GCP Pub/Subでtopicにpublishする.  
  __Source code:[AboutMe](/AboutMe.md)__  
  
 #### 8.twitter_output (python 3.7) 
 * フォーマットされた予測結果をツイートする.  
  __Source code:[twitter_output](/functions/twitter_output/main.py)__  
  
 #### 9.predict_result_aggregate (python 3.7) 
 * 予測したレースの結果を集計する.  
 * 以下の観点で結果を集計する.   
   - 軸馬として選んだ馬の馬券内率
   - 開催場ごとの的中率と回収率
 * 集計した結果をjsonファイルに出力してGCP Storageにアップロードする.  
  __Source code:[predict_result_aggregate](/functions/predict_result_aggregate/main.py)__  
  
 #### 11.predict_result_output (python 3.7) 
 * GCP Storageにアップロードされた予測したレースの集計結果のjsonファイルを参照し、結果を出力する.  
 * 集計結果のフォーマットを整えて、GCP Pub/Subでtopicにpublishする. 

  __Source code:[predict_result_output](/functions/predict_result_output/main.py)__  

## 関連モジュール  
モデル学習用のデータを取得したスクレイピングモジュール.  
モデルのトレーニングモジュール  
### GCP サービス  

* Scheduler  

| Scheduler | 概要 | 頻度 | ターゲット | ターゲットFunctions |  
----|----|----|----|----   
| predict_race_get | 特徴量に必要なデータの収集 | 毎週土日 0:00 | HTTP | week_race_get |  
| predict_race_check | 予測対象レースの特徴量作成と予測 | 毎週土日 9:00~15:40 20分毎 | HTTP | predict_race_check |  
| predict_result_aggregate | 予測結果の集計 | 毎週土日 19:00 | HTTP | predict_result_aggregate |  

* Storage bucket  

| bucket | オブジェクト概要 | trigger | ファイルをアップロードするFunctions |  
----|----|----|----   
| race_info | レースの条件(開催場、距離、...)データのファイル | - | past_race_get |  
| race_result | レース結果データのファイル | - | past_race_get |  
| race_pay | 払い戻し結果データのファイル | - | past_race_get |  
| race_order | 出走表データのファイル | - | predict_race_check |  
| race_result_html | parseしたHTMLファイル | - | past_race_get |  
| predict_race | 予測対象のレースの条件データのファイル<br>予測対象レース一覧ファイル | - | predict_race |  
| predict_horse | 予測対象に出走する競走馬のファイル | - | predict_horse |  
| run_predict | 予測を行うレースの出走表データのファイル | featuer_make | predict_race_check |  
| race_featuer | モデルに流す特徴量ファイル | predict_result | featuer_make |  
| predict_result_aggregate | レース毎に予測と結果をまとめたファイル | - | predict_result_aggregate |  
| race_pred_result | モデルの予測値と予測結果をまとめたファイル | predict_result_output | predict_result |  

* BigQuery Table  

| Table | 外部データ参照先 | 概要 |  
----|----|----   
| race_info | Storage::race_info | レースの条件 |  
| race_result | Storage::race_result | レース結果 |  
| race_pay | Storage::race_pay | 払い戻し結果 |  

* Pub/Sub Topic    

| Topic | Publish内容 | trigger | PublishするFunctions |   
----|----|----|----   
| predict_race | 開催週のレースURL一覧 | predict_race | week_race_get |  
| predict_horse | 予測対象に出走する競走馬のURL一覧 | predict_horse | predict_race |  
| past_race | 予測対象に出走する競走馬の過去に出走したレース結果のURL一覧(ストレージに存在するデータを除く) | past_race_get | predict_horse |  
| tweet_msg | Tweetする内容 | predict_result_output | predict_result<br>predict_result_output |  
### TODO  
* 予測モデルの精度向上
* デプロイモジュールの作成
* 土日開催以外の場合、どうするか

