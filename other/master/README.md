 netkeibaから期間指定してデータを取得
 - rece_result.[csv,tsv,json]
 - race_info.[csv,tsv,json]
 - race_odds.[csv,tsv,json]
 
` python netkeiba_race_master.py -s 2021-01-01 -e 2021-09-01 `

-s データ取得開始日

-e データ取得終了日

-o 出力ファイルの種類

csv、tsv、json

-u 新規作成/更新

new、update

新規作成の場合、出力ファイルの拡張子にタイムスタンプがつく

更新する場合は、実行ディレクトリに更新したいrece_result、race_inforace_oddsのファイルを置いておく

# テーブル定義
### race_result
| column_name | type | discription | 
| -----: | :------: | :----  |
| race_id | STRING | レース識別ID(FK)<br>netkeibaのIDと同じ |	
| horse_id | INTEGER | 馬識別ID(FK)<br>netkeibaのIDと同じ  |
| wakuban | INTEGER | 枠番|	
| umaban | INTEGER | 馬版|		
| horse_sex | STRING | 馬の性別|	
| horse_year | INTEGER | 馬の年齢|
| jockey | STRING | ジョッキー名 |	
| kinryo | FLOAT | 斤量(乗るジョッキーの体重)|	
| odds | FLOAT | 確定払い戻し単勝オッズ|	
| Popular | INTEGER | 確定レース人気|	
| time | FLOAT | 走破タイム|
| last3f | FLOAT | 上がり3Fのタイム|	
| conner1 | INTEGER	| 1コーナー通過順位|	
| conner2	| INTEGER	| 2コーナー通過順位|	
| conner3	| INTEGER	| 3コーナー通過順位|
| conner4	| INTEGER	| 4コーナー通過順位|
| tyaku | INTEGER | ゴール番通過順位 |
| horse_weight	| INTEGER	| 馬体重|	
| dif_horse_weight| INTEGER	| 前走との馬体重差|

### race_info
| column_name | type | discription | 
| -----: | :------: | :----  |
| race_id | STRING | レース識別ID(FK)<br>netkeibaのIDと同じ |	
| race_num | INTEGER | レース番号  |
| race_name | INTEGER | レース名|	
| race_date | INTEGER | レース開催日|		
| race_rank1 | STRING | 出走条件(年齢)|	
| race_rank2 | INTEGER | 出走条件(新馬/未勝利/1勝/2勝/3勝/オープン)|
| distance | STRING | 距離 |	
| kind | FLOAT | 芝/ダート|	
| place | FLOAT | 競馬場|	
| horse_count | INTEGER | 出走頭数|	
| lap_time | FLOAT | レースラップタイム|
| conner1 | FLOAT | 1コーナー通過順|	
| conner2 | INTEGER	| 2コーナー通過順|	
| conner3 | FLOAT | 3コーナー通過順|	
| conner3 | INTEGER	| 4コーナー通過順|	

### race_odds
| column_name | type | discription | 
| -----: | :------: | :----  |
| race_id | STRING | レース識別ID(FK)<br>netkeibaのIDと同じ |	
| indicator | STRING | 券種  |
| horse_1 | INTEGER | 馬券に絡んだ馬1 |	
| horse_2 | INTEGER | 馬券に絡んだ馬2 |		
| horse_3 | STRING | 馬券に絡んだ馬3 |	
| pay | INTEGER | 払戻金|
	