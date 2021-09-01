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
source = 'https://dinkes.kerincikab.go.id/informasi-covid-19/'
driver.get(source)
time.sleep(5)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
driver.quit()

td = soup.find_all('td')
txt = [i.get_text() for i in td]
for i in range(64):
    txt.remove('')
for i in range(12):
    txt.remove('Orang')
for i in range(19):
    txt.remove(':')
tgl = txt[1].split()[-7:-4]
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(tgl[1].lower())+1
date_update = tgl[2]+'-'+str(bln)+'-'+tgl[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

data = {'total_pdp':[txt[6]],
        'pdp_dipantau':[txt[9]],
        'pdp_sembuh':[txt[12]],
        'total_positif':[txt[17]],
        'positif_dirawat':[txt[20]],
        'positif_sembuh':[txt[23]],
        'positif_meninggal':[txt[29]],
        'positif_isolasi':[int(txt[34]) + int(txt[37]) + int(txt[40])]} 
df = pd.DataFrame(data) 
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value ='Jambi')
df.insert(loc=3, column='kabkot', value = 'Kerinci')
df.insert(loc=12, column='source_link', value = source)
df.insert(loc=13, column='types', value = 'kabkot')
df.insert(loc=14, column='user_pic', value ='Dea')
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
        print(date + " Kabupaten Kerinci Done")