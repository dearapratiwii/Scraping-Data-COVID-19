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
source = 'https://alfiyan-mustaqim.maps.arcgis.com/apps/opsdashboard/index.html#/d35570fb71e649e08a7234a1829bc79a'
driver.get(source)
time.sleep(60)

content = driver.page_source
soup = BeautifulSoup(content, 'html.parser')


div = soup.find('div', class_ = "subtitle text-ellipsis no-pointer-events margin-right-half").get_text().split()[1:4]
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(div[1].lower())+1
date_update = div[2]+'-'+str(bln)+'-'+div[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")
driver.quit()

span = soup.find_all('span')
links = span[9:]
kecamatan = []
kelurahan = []
total_positif = []
total_pdp = []
kecamatan2 = []
total_positif2 = []
total_pdp2 = []
for i, link in enumerate(links):
    if i in range(0, 108, 9):
        kecamatan.append(link.get_text().lower().capitalize().split())
    if i in range(3, 108, 9):
        total_positif.append(int(link.get_text().replace('Terkonfirmasi:\xa0', '', 1)))
    if i in range(7, 108, 9):
        total_pdp.append(int(link.get_text().replace('Suspek:\xa0', '', 1)))
    if i in range(109, 1792, 11): #sampai 1941
        kelurahan.append(link.get_text().lower().capitalize().split())
    if i in range(111, 1792, 11):
        kecamatan2.append(link.get_text().lower().capitalize().split())
    if i in range(112, 1792, 11):
        total_positif2.append(link.get_text().replace('Terkonfirmasi :\xa0', '', 1))
    if i in range(117, 1792, 11):
        total_pdp2.append(link.get_text().replace('Suspek Aktif:\xa0', '', 1))

lind = []
for sublist in kecamatan:
    for item in sublist:
        lind.append(item)
lim = []
for sublist in kelurahan:
    for item in sublist:
        lim.append(item)
linb = []
for sublist in kecamatan2:
    for item in sublist:
        linb.append(item)
        
kecamatan = []
kelurahan = []
kecamatan2 = []
for i, link in enumerate(lind):
    if i in range(1, len(lind), 9):
        kecamatan.append(link.lower().capitalize())
for i, link in enumerate(lim):
    if i in range(0, len(lim), 10):
        kelurahan.append(link.lower().capitalize())
for i, link in enumerate(linb):
    if i in range(1, len(linb), 2):
        kecamatan2.append(link.lower().capitalize())
        
df = pd.DataFrame()
df['kecamatan'] = kecamatan
df['total_positif'] = total_positif
df['total_pdp'] = total_pdp
df.insert(loc=0, column='scrape_date', value= date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value='Jawa Tengah')
df.insert(loc=3, column='kabkot', value = 'Sukoharjo')
df.insert(loc=7, column='source_link', value = 'https://corona.sukoharjokab.go.id/')
df.insert(loc=7, column='types', value='kecamatan')
df.insert(loc=9, column='user_pic', value='Dea')
df

dff = pd.DataFrame()
dff['kecamatan'] = kecamatan2
dff['kelurahan'] = kelurahan 
dff['total_positif'] = total_positif2
dff['total_pdp'] = total_pdp2
dff.insert(loc=0, column='scrape_date', value= date)
dff.insert(loc=1, column='date_update', value = date_update)
dff.insert(loc=2, column='provinsi', value='Jawa Tengah')
dff.insert(loc=3, column='kabkot', value = 'Sukoharjo')
dff.insert(loc=8, column='source_link', value = 'https://corona.sukoharjokab.go.id/')
dff.insert(loc=8, column='types', value='kelurahan')
dff.insert(loc=10, column='user_pic', value='Dea')
dff

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
        print(date + " Kabupaten Sukoharjo Kecamatan Done")

try:
    mydb = mysql.connector.connect(host='db-blanja2',
                                   port=3306,
                                   user='covid_user',
                                   passwd='5bb6593aa078',
                                   database='covid')
    cursor = mydb.cursor()
    
    cols1 = "`,`".join([str(i) for i in dff.columns.tolist()])
    # Insert DataFrame records one by one.
    for i,row in dff.iterrows():
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
        print(date + " Kabupaten Sukoharjo Kelurahan Done")

import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')
