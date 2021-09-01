from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime as dt
import time
import io
import pdfplumber
import requests
import io

option = webdriver.ChromeOptions()
option.add_argument('--headless')
option.add_argument('--no-sandbox')
option.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver', options = option)
source = 'https://covid19.bengkuluprov.go.id/databengkulu'
driver.get(source)
time.sleep(5)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

date_update = dt.strptime(soup.find_all('td')[1].get_text().split()[0],'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

a = soup.find_all('a', href = True)[12]
link1 = a['href']
driver.get(link1)
time.sleep(5)
content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
link1

i = soup.find('iframe')
link2 = i['src']
link2

response = requests.get(link2)
with io.BytesIO(response.content) as f:
    with pdfplumber.open(f) as pdf:
        page = pdf.pages[1]
        txt = page.extract_text()
links = txt.replace('COVID-19 WNA India', '', 1).replace('\nTanggal 5 Mei 2021\npada Kapal MV.', '', 1).replace('SPESIMEN VISHVA DIKSA,semua', '', 1).replace('WNA tersebut telah\npulang kembali ke Peta Zonasi Resiko bersumber \n52.645 \n(+840 )\nIndia data dari www.covid19.go.id @ TimData COVID-19DinkesProv Bengkulu', '', 1).replace('\n', ' ', 1000).replace('dariKasusKonfirmasi', '', 1).replace('spesimen diperiksa', '', 1).replace('PROVINSI BENGKULU','',1).replace(':', "", 100).replace('KONFIRMASI', " ", 100).replace('SEHAT99', '', 1).replace('SEMBUH', '', 100).replace('KASUS', '', 100).replace('MENINGGAL', '', 100).replace('SUSPEK', '', 100).replace('KasusKonfirm', '', 100).replace('Kasus', '', 100)
l = links.split()
l[0:2] = [' '.join(l[0:2])]
del(l[1:3])
del(l[5])
del(l[6])
del(l[13:15])
l

kabkot = ['Mukomuko', 'Bengkulu Utara', 'Lebong', 'Rejang Lebong', 'Bengkulu Tengah',
         'Kepahiang', 'Kota Bengkulu', 'Seluma', 'Bengkulu Selatan', 'Kaur']

total_positif = [l[3], l[2], l[14], l[13], l[25], 
                 l[32], l[41], l[44], l[52], l[71]]
positif_sembuh = [l[5], l[4], l[16], l[15], l[26], 
                 l[34], l[43], l[51], l[56], l[78]]
positif_meninggal = [l[7], l[6], l[18], l[17], l[31], 
                 l[35], l[50], l[55], l[61], l[79]]
total_pdp = [l[9], l[8], l[23], l[19], l[33], 
             l[38], l[54], l[57], l[65], l[80]]

df = pd.DataFrame()
df['kabkot'] = kabkot
df['total_positif'] = total_positif
df['positif_meninggal'] = positif_meninggal
df['positif_sembuh'] = positif_sembuh
df['total_pdp'] = total_pdp
df.insert(0,column ='scrape_date', value= date)
df.insert(1, column ='date_update', value = date_update)
df.insert(2, column ='provinsi', value = 'Bengkulu')
df.insert(8,column='types', value = 'kabkot')
df.insert(9,column='source_link', value = source)
df.insert(10,column='user_pic', value = 'Dea')
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
        print(date + " Provinsi Bengkulu Kabkot Done")
