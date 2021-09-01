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
source = 'https://covid19.bungokab.go.id/monitoring'
driver.get(source)
time.sleep(15)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

span = soup.find('span', class_ = 'badge badge-danger').get_text().split()[1:4]
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli',
         'agustus', 'september', 'oktober','november','desember']
bln = bulan.index(span[1].lower())+1
date_update = span[2]+'-'+str(bln)+'-'+span[0]
date_update = dt.strptime(date_update,'%Y-%M-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

div = soup.find_all('div', class_ = 'col-sm-12 col-md-12 col-lg-4')
links = [i.find_all('td') for i in div]

kecamatan = []
total_odp = []
total_pdp = []
pdp_sembuh = []
total_otg = []

for i, link in enumerate(links[0]):
    if i in range(1, len(links[0]), 3):
        kecamatan.append(link.get_text().lower().capitalize())
    if i in range(2, len(links[0]), 3):
        total_odp.append(link.get_text())
for i, link in enumerate(links[4]):
    if i in range(2, len(links[4]), 3):
        total_pdp.append(link.get_text())
for i, link in enumerate(links[3]):
    if i in range(2, len(links[3]), 3):
        pdp_sembuh.append(link.get_text())
for i, link in enumerate(links[6]):
    if i in range(2, len(links[6]), 3):
        total_otg.append(link.get_text())
        
df = pd.DataFrame()
df['kecamatan'] = kecamatan
df['total_odp'] = total_odp
df["total_pdp"] = total_pdp
df["pdp_sembuh"] = pdp_sembuh
df["total_otg"] = total_otg
df['source_link'] = source
df.insert(0,column='scrape_date', value = date)
df.insert(1,column='date_update', value = date_update)
df.insert(2,column='provinsi', value='Jambi')
df.insert(3,column='kabkot', value='Bungo')
df.insert(9,column='types', value='kecamatan')
df.insert(11,column='user_pic', value='Dea')

df['total_odp'] = df['total_odp'].astype(int)
df['total_pdp'] = df['total_pdp'].astype(int)
df['pdp_sembuh'] = df['pdp_sembuh'].astype(int)
df['total_otg'] = df['total_otg'].astype(int)

kot = df.sum(axis = 0)
df.loc[-1] = [date, date_update, 'Jambi', 'Bungo', '', kot[5], kot[6], kot[7], kot[8],
            'kabkot', 'https://covid19.bungokab.go.id/monitoring', 'dea']
df.index = df.index + 1
df = df.sort_index()
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
        print(date + " Kabupaten Bungo Done")

import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')
