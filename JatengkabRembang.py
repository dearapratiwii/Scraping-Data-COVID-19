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
source = 'http://sim.rembangkab.go.id/portal/home/corona/full'
driver.get(source)
time.sleep(30)
content = driver.page_source
page = BeautifulSoup(content, 'html.parser')
content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

date_update = soup.find('h1').get_text().split()[6].replace('/', '-', 2)
date = dt.now().strftime("%Y-%m-%d")
tbody = soup.find('tbody')
links = tbody.find_all('td')
provinsi = []
kabkot = []
kecamatan = []
total_positif = []
positif_meninggal = []
positif_dirawat = []
positif_isolasi = []
total_odp = []
total_pdp = []
for i, link in enumerate(links):
    if i in range(0, len(links), 16):
        kecamatan.append(link.get_text().lower().capitalize())
    if i in range(1, len(links), 16):
        total_odp.append(link.get_text())
    if i in range(2, len(links), 16):
        total_pdp.append(link.get_text())
    if i in range(7, len(links), 16):
        positif_dirawat.append(link.get_text())
    if i in range(12, len(links), 16):
        positif_isolasi.append(link.get_text())
    if i in range(13, len(links), 16):
        total_positif.append(link.get_text())
    if i in range(15, len(links), 16):
        positif_meninggal.append(link.get_text())

df = pd.DataFrame()
df['kecamatan'] = kecamatan
df['total_positif'] = total_positif
df['positif_meninggal'] = positif_meninggal
df['positif_dirawat'] = positif_dirawat
df['positif_isolasi'] = positif_isolasi
df['total_odp'] = total_odp
df['total_pdp'] = total_pdp
df.insert(loc=0, column='scrape_date', value= date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value='Jawa Tengah')
df.insert(loc=3, column='kabkot', value = 'Rembang')
df.insert(loc=11, column='source_link', value= source)
df.insert(loc=11, column='types', value='kecamatan')
df.insert(loc=13, column='user_pic', value='Dea')

h3 = soup.find_all('h3')
h3 = [i.get_text().replace('\n                    ', '', 1) for i in h3]
df.insert(7,column='positif_sembuh', value='')
df.loc[-1] = [date, date_update, 'Jawa Tengah', 'Rembang', '',
              h3[7].replace('                    Total Terkonfirmasi\n', '', 1),
              h3[5].replace('                    Terkonfirmasi Meninggal\n', '', 1),
              h3[4].replace('\nTerkonfirmasi Sembuh\n', '', 1).split()[1].replace('+', '', 1),
              h3[2].replace('                    Sisa Konfirmasi Simtomatik\n', '', 1),
              h3[3].replace('                    Sisa Konfirmasi Asimtomatik\n', '', 1),
              h3[0].replace('\nSisa Kontak Erat\n', '', 1).split()[1],
              h3[1].replace('                    Sisa Suspek\n', '', 1), 'kabkot', 
              source, 'dea']
df.index = df.index + 1
df = df.sort_index()
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
        print(date + " Kabupaten Rembang Done")

import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')