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
source = 'https://wonogirikab.go.id/index.php/info-corona/'
driver.get(source)
time.sleep(10)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
driver.quit()

p = soup.find_all('p')[1].get_text().split()[4:7]
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(p[1].lower())+1
date_update = p[2]+'-'+str(bln)+'-'+p[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

td = soup.find_all('td')
h4 = soup.find_all('h4')
h4 = [i.get_text() for i in h4]
h3 = soup.find_all('h3')
h3 = [i.get_text() for i in h3]
a = h4[6:8]
a = [i.replace('.', '', 1) for i in a]
for i in range(0, len(a)):
    a[i] = int(a[i])
    
data = {'total_pdp':[h3[8]],
        'pdp_dipantau':[h3[10] + h3[9]],
        'pdp_sembuh':[h3[11].replace(' Kasus', '', 1)],
        'pdp_meninggal':[h3[12].replace(' Kasus', '', 1)],
        'total_positif':[a[0]],
        'positif_sembuh':[h3[5].replace(' Kasus', '', 1).replace('.','',1)],
        'positif_dirawat':[h3[3]],
       'positif_isolasi':[h3[4]],
       'positif_meninggal':[h3[6].replace(' Kasus', '', 1)]} 
df = pd.DataFrame(data) 

df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value='Jawa Tengah')
df.insert(loc=3, column='kabkot', value = 'Wonogiri')
df.insert(loc=13, column='types', value='kabkot')
df.insert(loc=14, column='source_link', value='https://wonogirikab.go.id/index.php/info-corona/')
df.insert(loc=15, column='user_pic', value='Dea')
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
        print(date + " Kabupaten Wonogiri Done")

import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')