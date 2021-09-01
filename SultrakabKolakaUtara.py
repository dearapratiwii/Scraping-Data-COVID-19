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
source = 'https://berita.kolutkab.go.id/update-data-kewaspadaan-covid-19-kolaka-utara-2020/'
driver.get(source)
time.sleep(40)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

#date = soup.find('figcaption').get_text().replace('.', '', 1).split()[2:5]
#bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
#bln = bulan.index(date[1].lower())+1
#date_update = date[2]+'-'+str(bln)+'-'+date[0]
#date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date_update = dt.now().strftime("%Y-%m-%d")
date = dt.now().strftime("%Y-%m-%d")

tabel = soup.find('table', class_ = 'has-fixed-layout')
links = tabel.find_all('td')

scrape_date = []
provinsi = []
kabkot = []
kecamatan = []
total_odp = []
total_pdp = []
total_positif = []
total_otg = []
source_link = []
user_pic = []
for i, link in enumerate(links):
    if i in range(0, len(links), 5):
        kecamatan.append(link.get_text().lower().capitalize())
    if i in range(1, len(links), 5):
        total_positif.append(link.get_text())
    if i in range(2, len(links), 5):
        total_otg.append(link.get_text())
    if i in range(3, len(links), 5):
        total_pdp.append(link.get_text())
    if i in range(4, len(links), 5):
        total_odp.append(link.get_text())
    if i in range(1, len(links), 5):
        scrape_date.append(date)
    if i in range(1, len(links), 5):
        source_link.append(source)
    if i in range(1, len(links), 5):
        user_pic.append('dea')

df = pd.DataFrame()
df['scrape_date'] = scrape_date
df['date_update'] = date_update
df['kecamatan'] = kecamatan
df['total_otg'] = total_otg
df['total_odp'] = total_odp
df['total_pdp'] = total_pdp
df['total_positif'] = total_positif
df['source_link'] = source_link
df['user_pic'] = user_pic
df.insert(2,column='provinsi', value='Sulawesi Tenggara')
df.insert(3,column='kabkot', value='Kolaka Utara')
df.insert(9,column='types', value='kecamatan')

figure = soup.find('figure', class_ = 'wp-block-table is-style-regular')
td = figure.find_all('td')
text = [i.get_text() for i in td]
for i in range(0, len(text)): 
    text[i] = int(text[i])
figure2 = soup.find_all('figure', class_ = 'wp-block-table')[1]
td2 = figure2.find_all('td')
text2 = [i.get_text() for i in td2]
for i in range(0, len(text2)): 
    text2[i] = int(text2[i])

df.insert(9,column='positif_sembuh', value='')
df.insert(10,column='positif_dirawat', value='')
df.insert(9,column='positif_meninggal', value='')
df.loc[-1] = [date, date_update, 'Sulawesi Tenggara', 'Kaloka Utara', '', text[1], text[3], text[2], text[0], text2[1], text2[0], text2[1],
              'kabkot', 'https://berita.kolutkab.go.id/update-data-kewaspadaan-covid-19-kolaka-utara-2020/', 'Dea']
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
        print(date + " Kabupaten Kolaka Utara Done")
        
import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')
