from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime as dt

option = webdriver.ChromeOptions()
option.add_argument('--headless')
option.add_argument('--no-sandbox')
option.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver', options = option)
source = 'http://covid19.meranginkab.go.id/'
driver.get(source)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

h4 = soup.find('h4', class_ = 'elementor-heading-title elementor-size-default').get_text().split()[5:8]
bulan = ['januari', 'pebruari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(h4[1].lower())+1
date_update = h4[2]+'-'+str(bln)+'-'+h4[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

div = soup.find_all('div', class_ = 'elementor-text-editor elementor-clearfix')
p = [i.find_all('p') for i in div]
links = []
for sublist in p:
    for item in sublist:
        links.append(item)
angka = []
for i, link in enumerate(links):
    if i in range(1, len(links), 3):
        angka.append(link.get_text().replace('.', '', 1))
for i in range(0, len(angka)): 
    angka[i] = int(angka[i])
    
data = {'total_odp':[angka[0]],
        'total_pdp':[angka[1]],
        'total_positif':[(angka[2]) + angka[3] + angka[4]],
        'positif_sembuh':[angka[3]],
        'positif_meninggal':[angka[4]],} 
df = pd.DataFrame(data) 

df.insert(loc=0, column='scrape_date', value= date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value='Jambi')
df.insert(loc=3, column='kabkot', value = 'Merangin')
df.insert(loc=9, column='source_link', value='http://covid19.meranginkab.go.id/')
df.insert(loc=10, column='types', value='kabkot')
df.insert(loc=11, column='user_pic', value='Dea')
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
        print(date + " Kabupaten Merangin Done")

import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')
