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
source = 'http://corona.sultraprov.go.id/'
driver.get(source)
time.sleep(10)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

s = soup.find('span', class_ = 'subheading').get_text().split()[3:6]
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(s[1].lower())+1
date_update = s[2]+'-'+str(bln)+'-'+s[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

b = soup.find_all('b')[0:7]
b = [int(i.get_text()) for i in b]
sup = soup.find_all('sup')
p = [int(i.get_text().replace('(+', '', 1).replace(')', '', 1).replace('(-', '', 1)) for i in sup]
data = {'positif_sembuh':[p[1]],
        'total_positif':[b[0]],
       'positif_dirawat':[b[1]],
       'positif_meninggal':[b[3]],
       'types':['provinsi'],
       'source_link':['http://corona.sultraprov.go.id/']} 
df1 = pd.DataFrame(data)
df1

source = 'http://corona.sultraprov.go.id/front/data2'
driver.get(source)
time.sleep(5)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

sultra = []
table = soup.find('table',{'class':'table table-bordered'})
table_rows = table.findAll('tr')
for tr in table_rows[4:]:
    td = tr.findAll('td')
    row = [i.get_text().replace('\n', '', 2).replace('.', '', 1).replace('KAB', '', 1) for i in td]
    sultra.append(row)
hasil = pd.DataFrame(sultra)
tbody = soup.find('tbody')
r = tbody.find_all('td')
links = r[1:153]
kabkot = []
total_odp = []
pdp_isolasi = []
total_positif = []
positif_sembuh = []
positif_dirawat = []
positif_meninggal = []

suspek = []
probable = []
for i, link in enumerate(links):
    if i in range(0, len(links), 9):
        kabkot.append(link.get_text().replace('Kabupaten ', '', 1).lower().title())
    if i in range(1, len(links), 9):
        pdp_isolasi.append(link.get_text().split()[0])
    if i in range(2, len(links), 9):
        probable.append(link.get_text().split()[0])
    if i in range(3, len(links), 9):
        total_odp.append(link.get_text().split()[0])
    if i in range(4, len(links), 9):
        positif_sembuh.append(link.get_text().split()[0])
    if i in range(5, len(links), 9):
        positif_meninggal.append(link.get_text().split()[0])
    if i in range(6, len(links), 9):
        positif_dirawat.append(link.get_text().split()[0])
    if i in range(7, len(links), 9):
        total_positif.append(link.get_text().split()[0])
df2 = pd.DataFrame()
df2['kabkot'] = kabkot
df2['total_odp'] = total_odp
df2['total_odp'].astype(int)
df2['pdp_isolasi'] = pdp_isolasi
df2['probable'] = probable
df2["total_pdp"] = df2["pdp_isolasi"].astype(int) + df2["probable"].astype(int)
del df2['probable']
df2["total_pdp"].astype(int)
df2['total_positif'] = total_positif
df2['positif_sembuh'] = positif_sembuh
df2['positif_meninggal'] = positif_meninggal
df2['positif_dirawat'] = positif_dirawat
df2['types'] = 'kabkot'
df2['source_link'] = source
df2

frames = [df1, df2]
df = pd.concat(frames, sort = True)
df.insert(0,column ='scrape_date', value= date)
df.insert(1, column ='date_update', value = date_update)
df.insert(2, column ='provinsi', value = 'Sulawesi Tenggara')
df.insert(12,column='user_pic', value='Dea')
df = df.fillna('')
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
        print(date + " Provinsi Sulawesi Tenggara Done")
        
import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')
