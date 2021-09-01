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
source = 'https://covid19.bengkuluprov.go.id/databengkulu'
driver.get(source)
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
time.sleep(30)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
driver.quit()

date_update = dt.strptime(soup.find('h3').get_text().split()[2],'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

span = soup.find_all('span')[0:12]
s = [i.get_text() for i in span]
data = {'total_odp':[s[0]],
        'odp_sembuh':[s[1]],
        'odp_meninggal':[s[9]],
        'total_pdp':[s[3]],
        'pdp_sembuh':[s[4]],
        'pdp_meninggal':[s[10]],
        'total_positif':[s[6]],
       'positif_meninggal':[s[11]],} 
df = pd.DataFrame(data) 
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value ='Bengkulu')
df.insert(loc=11, column='source_link', value = source)
df.insert(loc=12, column='types', value = 'provinsi')
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
        print(date + " Provinsi Bengkulu Done")
        
import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')
