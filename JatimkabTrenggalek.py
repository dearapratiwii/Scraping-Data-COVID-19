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
source = 'https://corona.trenggalekkab.go.id/'
driver.get(source)
time.sleep(5)
driver.find_element_by_xpath('//*[@id="nav-tabel-tab"]').click()

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

i = soup.find('i').get_text().split()[9]
date_update = dt.strptime(i,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

h1 = soup.find_all('h1')
h1 = [i.get_text().replace(' \n\t\t\t\t\t\t\t\t\t\t',
                           '',1).replace('                                        \n                                                                                (+',
                                        ' ', 1).replace(')                                        \n', '', 1).replace('\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t(+', ' ', 1).split() for i in h1]
p = []
for sublist in h1:
    for item in sublist:
        p.append(item)
data = {'positif_sembuh':[p[6]],
        'total_positif':[p[3]],
       'positif_dirawat':[p[4]],
       'positif_meninggal':[p[7]],
       'types':['kabkot']} 
df1 = pd.DataFrame(data)
df1

trg = []
table = soup.find('table',{'class':'table table-bordered table-hover mb-0 text-nowrap css-serial'})
table_rows = table.findAll('tr')
for tr in table_rows[1:15]:
    td = tr.findAll('td')
    row = [i.get_text().replace('\n', '', 2).replace('.', '', 1).replace('KAB', '', 1) for i in td]
    trg.append(row)
hasil = pd.DataFrame(trg)
import warnings
warnings.filterwarnings('ignore')
df2 = hasil[[1, 2, 3, 4, 5, 7]]
df2.columns = ['kecamatan','total_positif','positif_dirawat', 
               'positif_isoman', 'positif_karantina', 'positif_meninggal']
df2['kecamatan'].astype(str)
df2.loc[:,'kecamatan'] = df2["kecamatan"].str.title()
df2["positif_isolasi"] =  df2["positif_isoman"].astype(int) + df2["positif_karantina"].astype(int)
del(df2['positif_isoman'])
del(df2['positif_karantina'])
df2['types'] = 'kecamatan'
df2

frames = [df1, df2]
df = pd.concat(frames, sort = True)
df.insert(0,column ='scrape_date', value= date)
df.insert(1, column ='date_update', value = date_update)
df.insert(2, column ='provinsi', value = 'Jawa Timur')
df.insert(3, column ='kabkot', value = 'Trenggalek')
df.insert(11, column ='source_link', value = source)
df.insert(12,column='user_pic', value='Dea')
df = df.fillna('')
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
        print(date + " Kabupaten Trenggalek Done")
        
import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')