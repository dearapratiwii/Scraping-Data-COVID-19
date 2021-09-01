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
source = 'http://kolakatimurkab.go.id/pages/kawal-corona'
driver.get(source)
time.sleep(50)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

div = soup.find_all('div', attrs={'style': 'color:white;font-size:50pt'})
d = [i.get_text() for i in div]
td = soup.find_all('td')[3:6]
td = [i.get_text() for i in td]

data = {'pdp_dipantau':[td[2]],
        'pdp_sembuh':[td[1]],
        'pdp_meninggal':[td[0]],
        'total_odp':[d[3]],
       'positif_sembuh':[d[1]],
       'positif_dirawat':[d[0]],
       'positif_meninggal':[d[2]],} 
df = pd.DataFrame(data)
date = dt.now().strftime("%Y-%m-%d")
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date)
df.insert(loc=2, column='provinsi', value ='Sulawesi Tenggara')
df.insert(loc=3, column='kabkot', value = 'Kolaka Timur')
df.insert(loc=11, column='source_link', value =source)
df.insert(loc=12, column='types', value ='kabkot')
df.insert(loc=13, column='user_pic', value ='Dea')
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
        print(date + " Kabupaten Kolaka Timur Done")
        
import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')
