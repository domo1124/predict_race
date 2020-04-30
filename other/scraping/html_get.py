from bs4 import BeautifulSoup
import requests

url="https://db.netkeiba.com"
with open("url_list_20200407.txt",'rt') as f:
    for i in f:
        url_d = url+i
        race_id = i.replace('/','').replace('\n','')
        print(race_id)
        rr = requests.get(url_d)
        html_u = rr.content
        soup = BeautifulSoup(html_u, 'html.parser')
        file_name = "./data/{}.html".format(race_id)
        with open(file_name,'w', encoding = 'utf-8') as fw:
            fw.write(soup.prettify())