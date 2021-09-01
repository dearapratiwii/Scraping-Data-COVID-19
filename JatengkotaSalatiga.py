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
source = 'https://corona.salatiga.go.id/'
driver.get(source)
time.sleep(5)

content = driver.page_source
soup = BeautifulSoup(content, 'html.parser')
driver.quit()
p = soup.find('p', class_ = 'has-text-align-center').get_text().replace(',','',2).split()[3:6]
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(p[1].lower())+1
date_update = p[2]+'-'+str(bln)+'-'+p[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")
td = soup.find_all('td')[3:7]
td = [i.get_text().replace(" Orang", '', 1) for i in td]
for i in range(0, len(td)):
    td[i] = int(td[i])
h4 = soup.find('h4', class_ = 'has-text-color').get_text().split()[3:12]
del h4[1:3], h4[2], h4[3:5]
for i in range(0, len(h4)):
    h4[i] = int(h4[i].replace('.', '', 1))
    
data = {'total_odp':[td[0]],
        'total_pdp':[td[1]],
        'total_positif':[h4[0]],
       'positif_sembuh':[h4[2]],
       'positif_dirawat':[h4[1]],
       'positif_meninggal':[h4[3]],} 
df = pd.DataFrame(data) 
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value='Jawa Tengah')
df.insert(loc=3, column='kabkot', value = 'Kota Salatiga')
df.insert(loc=10, column='types', value='kabkot')
df.insert(loc=11, column='source_link', value='https://corona.salatiga.go.id/')
df.insert(loc=12, column='user_pic', value='Dea')
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
        print(date + " Kota Salatiga Done")
        
import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')