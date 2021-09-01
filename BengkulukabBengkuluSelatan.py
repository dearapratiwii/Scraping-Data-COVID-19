from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime as dt
import time

option = webdriver.ChromeOptions()
option.add_argument('--headless')
option.add_argument('--no-sandbox')
option.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver', options = option)
source = 'https://covid19.bengkuluselatankab.go.id/infokasus-bengkuluselatan/'
driver.get(source)
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(5)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

s = soup.find_all('strong')[1].get_text().split()[2:5]
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(s[1].lower())+1
date_update = s[2]+'-'+str(bln)+'-'+s[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

span = soup.find_all('span', class_ = 'percent-value')
p = [int(i.get_text()) for i in span]
data = {'total_pdp':[p[0] + p[6]],
        'pdp_dipantau':[p[4] + p[7]],
        'pdp_sembuh':[p[1] + p[10]],
        'pdp_meninggal':[p[3] + p[9]],
        'total_positif':[p[12]],
       'positif_sembuh':[p[15]],
       'positif_dirawat':[p[13]],
        'positif_isolasi':[p[14]],
       'positif_meninggal':[p[16]],} 
df = pd.DataFrame(data) 
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value ='Jawa Timur')
df.insert(loc=3, column='kabkot', value = 'Gresik')
df.insert(loc=13, column='source_link', value =source)
df.insert(loc=14, column='types', value ='kabkot')
df.insert(loc=15, column='user_pic', value ='Dea')
df

import mysql.connector

try:
    mydb = mysql.connector.connect(host='',
                                   port=,
                                   user='',
                                   passwd='',
                                   database='')
    cursor = mydb.cursor()
    
    cols1 = "`,`".join([str(i) for i in df.columns.tolist()])
    # Insert DataFrame records one by one. 
    for i,row in df.iterrows():
        sql = "INSERT INTO `covid19_data` (`" + cols1 + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)" 
        cursor.execute(sql, tuple(row)) 
        # the connection is not autocommitted by default, so we must commit to save our # changes 
        mydb.commit()

except mysql.connector.Error as error:
    print("Failed to insert into MySQL table {}".format(error))

finally:
    if (mydb.is_connected()):
        cursor.close()
        mydb.close()
        print(date + " Kabupaten Bengkulu Selatan Done")
        
import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')
