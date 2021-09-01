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
source = 'https://satgascovid19.gresikkab.go.id/'
driver.get(source)
time.sleep(15)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

#em = soup.find_all('em')[0].get_text().split()[3]
#b = soup.find_all('em')[1].get_text().split()[0:2]
#bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
#bln = bulan.index(b[0].lower())+1
#date_update = b[1]+'-'+str(bln)+'-'+em[0]
#date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")
date_update = dt.now().strftime("%Y-%m-%d")

p = soup.find_all('span', class_ = 'elementor-counter-number')
p = [i.attrs['data-to-value'] for i in p]
for i in range(0, len(p)): 
    p[i] = int(p[i])
data = {'total_pdp':[p[0] + p[2]],
        'pdp_dipantau':[p[1]],
        'pdp_sembuh':[p[3]],
        'total_positif':[p[4]],
       'positif_sembuh':[p[5]],
       'positif_meninggal':[p[6]],} 
df = pd.DataFrame(data) 
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value ='Jawa Timur')
df.insert(loc=3, column='kabkot', value = 'Gresik')
df.insert(loc=10, column='source_link', value =source)
df.insert(loc=11, column='types', value ='kabkot')
df.insert(loc=12, column='user_pic', value ='Dea')
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
        print(date + " Kabupaten Gresik Done")
