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
source = 'http://corona.pekalongankab.go.id/'
driver.get(source)
time.sleep(10)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
h2 = soup.find('h2', class_ = 'h5 mb-2 text-center').get_text().split()[3]
date_update = dt.strptime(h2,'%d-%m-%Y').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")
tbody = soup.find_all('tbody', class_ = 'text-left')
links = [i.find_all("td") for i in tbody]
kecamatan = []
probable = []
total_positif = []
suspek = []

for i, link in enumerate(links[0]):
    if i in range(0, len(links[0]), 2):
        kecamatan.append(link.get_text().lower().capitalize())
    if i in range(1, len(links[0]), 2):
        probable.append(link.get_text())
for i, link in enumerate(links[1]):
    if i in range(1, len(links[1]), 2):
        total_positif.append(link.get_text())
for i, link in enumerate(links[2]):
    if i in range(1, len(links[2]), 2):
        suspek.append(link.get_text())
kecamatan = [i.replace(', kabupaten pekalongan,', '', 1).replace('Kecamatan ', '', 1).replace(' kabupaten pekalongan, ', '', 1).capitalize() for i in kecamatan]
for i in range(0, len(suspek)): 
    suspek[i] = int(suspek[i])
for i in range(0, len(probable)): 
    probable[i] = int(probable[i])
df = pd.DataFrame()
df['kecamatan'] = kecamatan
df['probable'] = probable
df['suspek'] = suspek
df["total_pdp"] = df['suspek'] + df['probable']
del df['suspek']
del df['probable']
df["total_positif"] = total_positif
df['source_link'] = source
df.insert(0,column='scrape_date', value = date)
df.insert(1,column='date_update', value = date_update)
df.insert(2,column='provinsi', value='Jawa Tengah')
df.insert(3,column='kabkot', value='Pekalongan')
df.insert(8,column='types', value='kecamatan')
df.insert(9,column='user_pic', value='Dea')

h3 = soup.find_all('h3')
h3 = [i.get_text() for i in h3]
df.insert(7,column='pdp_dipantau', value ='')
df.insert(8,column='pdp_sembuh', value ='')
df.insert(9,column='pdp_meninggal', value ='')
df.insert(10,column='positif_dirawat', value ='')
df.insert(11,column='positif_sembuh', value ='')
df.insert(12,column='positif_isolasi', value ='')
df.insert(13,column='positif_meninggal', value ='')
total_pdp = int(h3[1]) + int(h3[6])
pdp_dipantau = int(h3[7]) + int(h3[2])
pdp_sembuh = int(h3[3]) + int(h3[8])
pdp_meninggal = int(h3[9]) + int(h3[4])
total_positif = h3[11]
positif_dirawat = h3[12]
positif_sembuh = h3[13]
positif_isolasi = h3[14]
positif_meninggal = h3[15]
df.loc[-1] = [date, date_update, 'Jawa Tengah', 'Pekalongan', '', total_pdp, total_positif, pdp_dipantau, pdp_sembuh, pdp_meninggal, positif_dirawat,
              positif_sembuh, positif_isolasi, positif_meninggal, 'http://corona.pekalongankab.go.id/', 'kabkot', 'dea']
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
        print(date + " Kabupaten Pekalongan Done")

import pymysql.cursors
import sqlalchemy
engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')