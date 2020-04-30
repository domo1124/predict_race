
import time
import re
import selenium
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains
import datetime
#from selenium.webdriver.firefox.options import Options
url_list = []



driver_path = "/home/h-kobayashi/bin/chromedriver"
URL = "http://db.netkeiba.com/?pid=race_search_detail"

driver = webdriver.Chrome(driver_path)

driver.implicitly_wait(30)
driver.get(URL)

element_t=driver.find_element_by_id("check_track_1")
element_d=driver.find_element_by_id("check_track_2")
#芝・ダート選別
element_t.click()
element_d.click()

years = driver.find_element_by_name('start_year')
yeare = driver.find_element_by_name('end_year')
# 取得したエレメントをSelectタグに対応したエレメントに変化させる
y_s = Select(years)
y_e = Select(yeare)

# 選択したいvalueを指定する
y_s.select_by_value('2016')
y_e.select_by_value('2020')


mons = driver.find_element_by_name('start_mon')
mone = driver.find_element_by_name('end_mon')
# 取得したエレメントをSelectタグに対応したエレメントに変化させる
m_s = Select(mons)
m_e = Select(mone)

# 選択したいvalueを指定する
m_s.select_by_value('1')
m_e.select_by_value('4')

co=1
idname_set=[]
idname = "check_Jyo_0"
idgre_set=[]
idgren="check_grade_"
while co < 10:
    od = '{0}{1}'.format(idname, co)
    idname_set.append(od)
    gg = '{0}{1}'.format(idgren, co)
    idgre_set.append(gg)
    od = ''
    gg=''
    co=co+1
for i in range(len(idname_set)):
    element_10=driver.find_element_by_id(idname_set[i])
    element_10.click()
for i in range(len(idgre_set)):
    element_10=driver.find_element_by_id(idgre_set[i])
    element_10.click()

element_10=driver.find_element_by_id("check_Jyo_10")
element_10.click()


element_12=driver.find_element_by_id("check_barei_12")
element_13=driver.find_element_by_id("check_barei_13")
element_14=driver.find_element_by_id("check_barei_14")

element_12.click()
element_13.click()
element_14.click()


driver.find_element_by_name('kyori_min').send_keys("1200")
driver.find_element_by_name('kyori_max').send_keys("2600")
l = driver.find_element_by_name('list')
ll = Select(l)

# 選択したいvalueを指定する
ll.select_by_value('100')
time.sleep(3)
driver.find_element_by_xpath('//input[@value="検索"]').click()
driver.maximize_window()
css_s = "[class='nk_tb_common race_table_01']"
co=1
while 1:
    time.sleep(5)
    WebDriverWait(driver, 60).until(EC.visibility_of_element_located((By.CSS_SELECTOR, css_s)))
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    result_table = soup.find_all("table",class_='nk_tb_common race_table_01')[0]
    trs = result_table.find_all("tr")
    data = trs[1:]#ヘッダー以外のデータ
    if len(data) == 0:
        break
    else:
        for tr in data:
            result = list(tr.find_all('td'))
            url_list.append(result[4].find("a").get("href"))
        print("{}ページ目".format(str(co)))
        co=co+1

        try:
            driver.execute_script("javascript:paging('{}');".format(co))
        except:
            break
driver.quit()
#txtに書き込み
with open('./url_list_{}.txt'.format(datetime.datetime.today().strftime('%Y%m%d %H:%M:%S')),'w') as f:
    f.write('\n'.join(url_list))

