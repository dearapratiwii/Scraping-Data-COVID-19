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
source = 'https://pikobar.jabarprov.go.id/distribution-case'
driver.get(source)
time.sleep(40)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
p = soup.find('p',
              class_ = 'pointer-events-none flex justify-between items-center').get_text().replace('\n                ',
                                                                                                       '', 1).replace('\n                ',
                                                                                                                     '', 1).split()
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(p[1].lower())+1
date_update = p[2]+'-'+str(bln)+'-'+p[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")
jabar = []
table = soup.find('table',{'class':'table w-full border-t border-solid border-gray-300 tableFixHead'})
table_rows = table.findAll('tr')
for tr in table_rows[4:]:
    td = tr.findAll('td')
    row = [i.get_text().replace('\n', '', 2).replace('.', '', 1).replace('KAB', '', 1) for i in td]
    jabar.append(row)
hasil = pd.DataFrame(jabar)
import warnings
warnings.filterwarnings('ignore')
df = hasil[[0, 3, 10]]
df.columns = ['kabkot','total_positif','positif_meninggal']
df['kabkot'].astype(str)
df.loc[:,'kabkot'] = df["kabkot"].str.title()
df.insert(0, column = 'scrape_date', value = date)
df.insert(1, column = 'date_update', value = date_update)
df.insert(2,column='provinsi', value='Jawa Barat')
df.insert(6,column='types', value='kabkot')
df.insert(7, column = 'source_link', value = source)
df.insert(8,column='user_pic', value='dea')
df

source = 'https://pikobar.jabarprov.go.id/'
driver.get(source)
time.sleep(15)
content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
p = soup.find('small', class_ = 'opacity-50').get_text().split()[3:6]
bulan = ['jan', 'feb', 'mar', 'apr', 'mei', 'jun','jul','ags', 'september', 'oktober','november','desember']
bln = bulan.index(p[1].lower())+1
date_update = p[2]+'-'+str(bln)+'-'+p[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")
div = soup.findAll('div', class_ = 'w-1/2')
div = [i.get_text().split() for i in div]
links = []
for sublist in div:
    for item in sublist:
        links.append(item)
links = [i.replace('.', '', 2) for i in links]
div2 = soup.find_all('div', class_ = 'text-2xl')[6].get_text().replace('.','',1).replace('\n                ', '', 1).replace('\n              ','',1)
df.insert(6, column='positif_isolasi', value='')
df.insert(7,column='positif_sembuh', value='')
df.insert(8,column='total_odp', value='')
df.insert(9,column='odp_sembuh', value='')
df.insert(10,column='total_pdp', value='')
df.insert(11,column='pdp_sembuh', value='')
df.insert(12, column = 'pdp_meninggal', value = '')
df.loc[-1] = [date, date_update, 'Jawa Barat', '', links[2], links[23], links[9], links[16], links[28], links[32],
              int(links[52]) + int(links[86]), int(links[56]) + int(links[90]), div2, 'provinsi', source, 'Dea']
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
        print(date + " Provinsi Jawa Barat Done")