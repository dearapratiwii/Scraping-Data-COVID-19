from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time

import pandas as pd
from datetime import datetime as dt
import datetime
import re
import pymysql
from sqlalchemy import create_engine
import mysql.connector

option = webdriver.ChromeOptions()
option.add_argument('--headless')
option.add_argument('--no-sandbox')
option.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver', options = option)
url='https://riwayat-file-covid-19-dki-jakarta-jakartagis.hub.arcgis.com/'
driver.get(url)
time.sleep(10)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
body = soup.find('body')
script = body.find('div',{'id':'ember80'})
src = script.findAll('a', href=True)

link = src[0].get('href')
rex = re.findall('/d/(.*)/view',link)
date_file = dt.now().strftime('%y%m%d')
df = pd.read_excel('https://drive.google.com/u/0/uc?id=197RC6IJBZQVfnDsnDF2-NhRUWl8OaNET&export=download'.format(rex[0]))

s = df.iloc[1]['Unnamed: 0'].replace('Tanggal ', '', 1).split()
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(s[1].lower())+1
date_update = s[2]+'-'+str(bln)+'-'+s[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

df = df.dropna()
del (df['Unnamed: 0'])
df.columns = ['kelurahan', 'total_positif', 'positif_sembuh', 'positif_isolasi', 'positif_dirawat', 'positif_meninggal',
               'total_suspek', 'suspek_dirawat', 'pdp_isolasi', 'suspek_sembuh', 'suspek_meninggal',
               'total_probable', 'probable_dirawat', 'probable_sembuh', 'probable_meninggal']
df["total_pdp"] = df["total_suspek"].astype(int) + df["total_probable"].astype(int)
df["pdp_dipantau"] = df["suspek_dirawat"].astype(int) + df["probable_dirawat"].astype(int)
df["pdp_sembuh"] = df["suspek_sembuh"].astype(int) + df["probable_sembuh"].astype(int)
df["pdp_meninggal"] = df["suspek_meninggal"].astype(int) + df["probable_meninggal"].astype(int)
del df['total_suspek']
del df['suspek_dirawat']
del df['suspek_sembuh']
del df['suspek_meninggal']
del df['total_probable']
del df['probable_dirawat']
del df['probable_sembuh']
del df['probable_meninggal']
df.insert(0,column ='scrape_date', value = date)
df.insert(1, column ='date_update', value = date_update)
df.insert(2, column ='provinsi', value = 'Jawa Tengah')
df.insert(3, column ='kabkot', value = 'Kota Surakarta')
df.insert(15, column ='types', value = 'kelurahan')
df.insert(16, column ='source_link', value = 'https://surakarta.go.id/?page_id=10806')
df.insert(17,column='user_pic', value='Dea')
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
        print(date + " Kota Surakarta Done")