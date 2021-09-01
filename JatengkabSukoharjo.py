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
source = 'https://corona.sukoharjokab.go.id/'
driver.get(source)
time.sleep(10)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

p2 = soup.find_all('p')[0]
date = p2.find('b')
date = date.get_text().split()[4:7]

bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(date[1].lower())+1
date_update = date[2]+'-'+str(bln)+'-'+date[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

p = soup.find_all('p', class_='card-text')
p = [i.get_text().replace('.', '', 1) for i in p]
for i in range(0, len(p)): 
    p[i] = int(p[i])

data = {'total_odp':[p[0]],
        'odp_dipantau':[p[3]],
        'odp_sembuh':[p[4]],
        'total_pdp':[p[5] + p[11]],
        'pdp_sembuh':[p[8] + p[13]],
        'pdp_dipantau':[p[7] + p[12]],
        'pdp_meninggal':[p[9] + p[14]],
        'total_positif':[p[15]],
       'positif_sembuh':[p[19]],
       'positif_dirawat':[p[18]],
       'positif_isolasi':[p[16]],
       'positif_meninggal':[p[20]],} 
df = pd.DataFrame(data)

df.insert(loc=0, column='scrape_date', value= dt.now().strftime("%Y-%m-%d"))
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value='Jawa Tengah')
df.insert(loc=3, column='kabkot', value = 'Sukoharjo')
df.insert(loc=16, column='source_link', value='https://corona.sukoharjokab.go.id/')
df.insert(loc=17, column='types', value='kabkot')
df.insert(loc=18, column='user_pic', value='Dea')
df

import mysql.connector

try:
    mydb = mysql.connector.connect(host='db-blanja2',
                                   port=3306,
                                   user='covid_user',
                                   passwd='5bb6593aa078',
                                   database='covid')
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
        print( date + " Kabupaten Sukoharjo Done")

import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')