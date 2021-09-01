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
source = 'https://covid19papuabarat.org/'
driver.get(source)
time.sleep(5)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

f = soup.find_all('font')[7].get_text().replace('/', '-', 2)
date_update = dt.strptime(f,'%d-%m-%Y').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

p = soup.find_all('font')[9:16]
p = [int(i.get_text().replace('+', '', 1).replace(' Hari Ini.', '', 1).replace('.', '', 1).replace('-', '', 1)) for i in p]
th = soup.find_all('th')
th = [i.get_text() for i in th]
data = {'total_positif':[p[0]],
       'positif_sembuh':[p[3]],
       'positif_meninggal':[p[5]],
       'positif_dirawat':[p[6]],
        'odp_meninggal':[th[-6]],
        'pdp_meninggal':[th[-5]],
       'types':['provinsi']} 
df1 = pd.DataFrame(data)
df1

positif_sembuh = []
positif_meninggal = []
total_positif = []
types = []
kabkot = []
positif_dirawat = []
links = soup.find_all('td')
for i, link in enumerate(links):
    if i in range(2, 183, 14):
        kabkot.append(link.get_text().lower().title())
    if i in range(5, 183, 14):
        total_positif.append(link.get_text().replace('.', '', 1))
    if i in range(7, 183, 14):
        positif_sembuh.append(link.get_text().replace('+', '', 1))
    if i in range(11, 183, 14):
        positif_meninggal.append(link.get_text())
    if i in range(12, 183, 14):
        positif_dirawat.append(link.get_text().replace('+', '', 1).replace('-', '', 1))
    if i in range(1, 183, 14):
        types.append('kabkot')
odp_meninggal = []
pdp_meninggal = []
links = links[183:len(links)]
for i, link in enumerate(links):
    if i in range(8, len(links), 15):
        odp_meninggal.append(link.get_text().replace('.', '', 1))
    if i in range(9, len(links), 15):
        pdp_meninggal.append(link.get_text().replace('+', '', 1))
df2 = pd.DataFrame()
df2['kabkot'] = kabkot
df2['total_positif'] = total_positif
df2['positif_sembuh'] = positif_sembuh
df2['positif_meninggal'] = positif_meninggal
df2['positif_dirawat'] = positif_dirawat
df2['odp_meninggal'] = odp_meninggal
df2['pdp_meninggal'] = pdp_meninggal
df2['types'] = types
df2

frames = [df1, df2]
df = pd.concat(frames, sort = True)
df.insert(0,column ='scrape_date', value= date)
df.insert(1, column ='date_update', value = date_update)
df.insert(2, column ='provinsi', value = 'Papua Barat')
df.insert(10, column ='source_link', value = source)
df.insert(11,column='user_pic', value='Dea')
df.iloc[0,3] = ''
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
        print(date + " Provinsi Papua Barat Done")
