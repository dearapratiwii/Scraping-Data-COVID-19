from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime as dt
import time
import tabula

"""
option = webdriver.ChromeOptions()
option.add_argument('--headless')
option.add_argument('--no-sandbox')
option.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver', options = option)
source = 'https://coronainfo.kaltaraprov.go.id/'
driver.get(source)
time.sleep(5)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

d = soup.find('div', class_ = 'widgetsubtitle').get_text().split()[4:7]
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(d[1].lower())+1
date_update = d[2]+'-'+str(bln)+'-'+d[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

a = soup.find_all('a', href = True)[4]
link1 = a['href']

driver.get(link1)
content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
a = soup.find('a', href = True)
link2 = a['href']
link2
"""
date = dt.now().strftime("%Y-%m-%d")
date_update = dt.now().strftime("%Y-%m-%d")
source = 'https://coronainfo.kaltaraprov.go.id/'

import logging
logger = logging.getLogger()
logger.setLevel(logging.CRITICAL)
dfs = tabula.read_pdf("/home/probis/notebook/corona/All/Dea/Manual/kaltara.pdf", stream=True, pages = 'all')

df11 = dfs[0][3:]
df1 = df11[['Unnamed: 0', 'SUSPEK', 'PROBABLE', 'SUSPEK DIISOLASI', 'SUSPEK DISCARDED' ]]
df1.columns = ['kabkot','suspek','probable', 'Nama', 'Name']
import warnings
warnings.filterwarnings('ignore')
df1[['6','pdp_sembuh']] = df1.Name.str.split(expand=True)
df1[['pdp_isolasi','8']] = df1.Nama.str.split(expand=True)
df1['total_pdp'] = df1['suspek'].astype(int) + df1['probable'].astype(int)
del (df1['suspek'])
del (df1['probable'])
del (df1['Name'])
del (df1['Nama'])
del (df1['6'])
del (df1['8'])
df1 = df1.reset_index(drop=True)
df1

df22 = dfs[1][2:]
df2 = df22[['Unnamed: 8', 'KASUS KONFIRMASI.1']]
df2.columns = ['total_positif', 'Name']
df2[['1','total_otg']] = df2.Name.str.split(expand=True)
del (df2['Name'])
del (df2['1'])
df2 = df2.reset_index(drop=True)

df33 = dfs[2][2:]
df3 = df33[['Unnamed: 5', 'BELUM SEMBUH &']]
df3.columns = ['positif_sembuh', 'Name']
df3[['1','positif_dirawat']] = df3.Name.str.split(expand=True)
del (df3['Name'])
del (df3['1'])
df3 = df3.reset_index(drop=True)
df3

df44 = dfs[3][3:]
df4 = df44[['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 4']]
df4.columns = ['total_odp', 'odp_dipantau', 'odp_sembuh']
df4 = df4.reset_index(drop=True)
df4

df55 = dfs[4][4:]
df5 = df55[['Unnamed: 6', 'Unnamed: 8']]
df5.columns = ['positif_meninggal', 'pdp_meninggal']
df5 = df5.reset_index(drop=True)
df5

frames = [df1, df2, df3, df4, df5]
df = pd.concat(frames, axis  = 1)
df.loc[3,'kabkot'] = ['Tana Tidung']
df.loc[5,'kabkot'] = ['']
df.loc[0,'total_odp'] = df.loc[0,'total_odp'].replace('ASI', '', 1)
df.loc[:,'kabkot'] = df.loc[:,'kabkot'].str.title()
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value ='Kalimantan Utara')
df.insert(loc=16, column='types', value ='kabkot')
df.loc[5,'types'] = ['provinsi']
df.insert(loc=17, column='source_link', value =source)
df.insert(loc=18, column='user_pic', value ='Dea')

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
        print(date + " Provinsi Kalimantan Utara Done")
        
import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')
