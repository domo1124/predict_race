## Overview  
1.netkeibaからデータを収集


## 実行手順  

 * 1.past_race_url_get.pyを実行して取得するレースのURL一覧ファイルを作成.  
 * 2.past_race_data_get.pyを実行する.  
	コマンドライン引数に１で作成したファイルのパスを入力する.  
 * 3.sqliteファイルが無い場合は、sqlite_table_make.pyを実行.  
 * 4.2で作成したファイルをGCSにアップロード

