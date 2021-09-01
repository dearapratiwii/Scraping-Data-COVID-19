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
source = 'https://surakarta.go.id/?page_id=10806'
driver.get(source)

time.sleep(3)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
driver.quit()

div = soup.find('div', class_ = 'quick_fact align_center animate-math')
tgl = div.find('span').get_text()
p = div.find('div', class_ = 'desc').get_text().split()
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(p[0].lower())+1
date_update = p[1]+'-'+str(bln)+'-'+tgl
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

span = soup.find_all('span', class_ = 'number')[1:4]
span = [i.get_text() for i in span]
for i in range(0, len(span)):
    span[i] = int(span[i])
h3 = soup.find_all('h3')
h3 = [i.get_text() for i in h3]
for i in range(0, len(h3)):
    h3[i] = int(h3[i])
    
data = {'total_pdp':[h3[4] + h3[5] + h3[8]],
        'pdp_sembuh':[h3[6] + h3[9]],
        'pdp_dipantau':[h3[4] + h3[8]],
        'pdp_isolasi':[h3[5]],
        'pdp_meninggal':[h3[7] + h3[10]],
        'total_positif':[span[0]],
       'positif_sembuh':[h3[0]],
       'positif_dirawat':[h3[2]],
       'positif_isolasi':[h3[1]],
       'positif_meninggal':[h3[3]],} 
df = pd.DataFrame(data) 

df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value='Jawa Tengah')
df.insert(loc=3, column='kabkot', value = 'Kota Surakarta')
df.insert(loc=14, column='types', value='kabkot')
df.insert(loc=15, column='source_link', value='https://surakarta.go.id/?page_id=10806')
df.insert(loc=16, column='user_pic', value='Dea')
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
        print(date + " Kota Surakarta Done")

import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')
