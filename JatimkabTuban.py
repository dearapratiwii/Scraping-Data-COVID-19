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
source = 'https://tubankab.go.id/page/informasi-tentang-virus-corona-covid-19'
driver.get(source)
time.sleep(30)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
p = soup.find_all('p')[19].get_text().split()
date_update = p[6].replace(',', '', 1) + '-' + p[5] + '-' + p[4]
date_update = dt.strptime(date_update,'%Y-%B-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

data = {'total_pdp':[p[17].replace(',', '', 1)],
        'total_positif':[p[33]],
       'positif_dirawat':[p[29].replace(',', '', 1)],
       'positif_meninggal':[p[25].replace(',', '', 1)],
       'types':['kabkot']} 
df1 = pd.DataFrame(data)
kec = soup.find_all('h6')[8:28]
kec = [i.get_text().replace('Kecamatan ', '', 1) for i in kec]
total_pdp = []
positif_meninggal = []
positif_dirawat = []
total_positif = []
types = []
links = soup.find_all('div', class_ = 'media-body')[18:118]
for i, link in enumerate(links):
    if i in range(0, len(links), 5):
        total_pdp.append(link.get_text().replace(' SUSPECT : ', '', 1))
    if i in range(2, len(links), 5):
        positif_meninggal.append(link.get_text().replace(' CONFIRM MENINGGAL : ', '', 1))
    if i in range(3, len(links), 5):
        positif_dirawat.append(link.get_text().replace(' CONFIRM DIRAWAT : ', '', 1))
    if i in range(4, len(links), 5):
        total_positif.append(link.get_text().replace(' TOTAL CONFIRM COVID-19 : ', '', 1))
    if i in range(1, len(links), 5):
        types.append('kecamatan')
df2 = pd.DataFrame()
df2['kecamatan'] = kec
df2['total_pdp'] = total_pdp
df2['total_positif'] = total_positif
df2['positif_dirawat'] = positif_dirawat
df2['positif_meninggal'] = positif_meninggal
df2['types'] = types

frames = [df1, df2]
df = pd.concat(frames, sort = True)
df.insert(0,column ='scrape_date', value= date)
df.insert(1, column ='date_update', value = date_update)
df.insert(2, column ='provinsi', value = 'Jawa Timur')
df.insert(3, column ='kabkot', value = 'Tuban')
df.insert(10, column ='source_link', value = source)
df.insert(11,column='user_pic', value='Dea')
df.iloc[0,4] = ''
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
        print(date + " Kabupaten Tuban Done")

import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')