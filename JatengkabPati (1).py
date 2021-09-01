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
source = 'https://covid19.patikab.go.id/v4/index.php/dataharian'
driver.get(source)
time.sleep(30)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
driver.quit()

small = soup.find('p', class_ = 'lh-copy measure black-60').get_text().split()[-3:]
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(small[1].lower())+1
date_update = small[2]+'-'+str(bln)+'-'+small[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

h3 = soup.find_all('h3', class_ = "black tl")
h3 = [int(i.get_text().replace(".", "", 1)) for i in h3[1:]]
s = soup.find_all('span', class_ = "text-muted black-40")
s = [i.get_text().replace(')', '', 1).replace(' (-', '', 1).replace(' (+', '', 1) for i in s[1:]]

data = {'total_positif':[h3[-1]],
       'positif_sembuh':[s[-2]],
       'positif_dirawat':[h3[0]],
        'positif_isolasi':[h3[1]],
       'positif_meninggal':[h3[-3]],
       'pdp_dipantau':[h3[3]],
       'pdp_sembuh':[h3[4]],
       'pdp_meninggal':[h3[5]]} 
df1 = pd.DataFrame(data)
df1.insert(loc=8, column='types', value = 'kabkot')
df1.insert(loc=9, column='source_link', value = source)
df1

driver = webdriver.Chrome('chromedriver', options = option)
source2 = 'https://covid19.patikab.go.id/v4/peta/peta_pati_kec'
driver.get(source2)
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

h6 = soup.find_all('h6')
h6 = [i.get_text().replace('Suspek : ', '', 1).replace('Konfirmasi : ', '', 1).replace(' Konfirmasi Dirawat:', '', 1).replace('\n', '', 1).replace(' \n', '', 1).split() for i in h6]
links = []
for sublist in h6:
    for item in sublist:
        links.append(item)
        
kecamatan = []
total_pdp = []
total_positif = []
positif_dirawat = []
for i, link in enumerate(links):
    if i in range(0, len(links), 4):
        kecamatan.append(link.lower().capitalize())
    if i in range(1, len(links), 4):
        total_pdp.append(int(link))
    if i in range(2, len(links), 4):
        total_positif.append(int(link))
    if i in range(3, len(links), 4):
        positif_dirawat.append(int(link))
df2 = pd.DataFrame()
df2['kecamatan'] = kecamatan
df2['total_pdp'] = total_pdp
df2['total_positif'] = total_positif
df2['positif_dirawat'] = positif_dirawat
df2['types'] = 'kecamatan'
df2['source_link'] = source2
df2

frames = [df1, df2]
df = pd.concat(frames, sort = True)
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value ='Jawa Tengah')
df.insert(loc=3, column='kabkot', value = 'Pati')
df.insert(loc=16, column='user_pic', value ='Dea')
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
        print(date + " Kabupaten Pati Done")
