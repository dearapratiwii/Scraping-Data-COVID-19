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
source = 'http://covid19.cirebonkab.go.id/'
driver.get(source)
time.sleep(30)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')
driver.quit()

div = soup.find('div', class_ = "mt-3 mb-5 wow fadeInUp")
span = div.find('span').get_text().split()[0:3]

bulan = ['january', 'february', 'march', 'april', 'may', 'june','july','august', 'september', 'october','november','december']
bln = bulan.index(span[1].lower())+1
date_update = span[2]+'-'+str(bln)+'-'+span[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

h6 = soup.find_all('h6')
text = [i.get_text().split() for i in h6]
list1 = []
for sublist in text:
    for item in sublist:
        list1.append(item)

div2 = soup.find_all('div', class_ = 'row justify-content-md-center')
td = [i.find_all('td') for i in div2]
text2 = td[1]
text2 = [i.get_text().split() for i in text2]
list2 = []
for sublist in text2:
    for item in sublist:
        list2.append(item)

list_angka = [list1[1], list1[8], list1[5], list1[14], list1[3], list2[84], 
              list1[25], list1[19], list1[21], list1[23], list1[35], list1[37], list1[39]]
for i in range(0, len(list_angka)): 
    list_angka[i] = int(list_angka[i])

data = {'total_odp':[list_angka[10]],
        'odp_dipantau':[list_angka[11]],
        'odp_sembuh':[list_angka[12]],
        'total_pdp':[list_angka[0]+list_angka[1]],
        'pdp_sembuh':[list_angka[4]],
        'pdp_dipantau':[list_angka[2]],
        'pdp_meninggal':[list_angka[3]],
        'total_positif':[list_angka[5]],
        'positif_sembuh':[list_angka[6]],
        'positif_dirawat':[list_angka[7]],
        'positif_isolasi':[list_angka[8]],
        'positif_meninggal':[list_angka[9]]} 
df = pd.DataFrame(data)

df.insert(loc=0, column='scrape_date', value= dt.now().strftime("%Y-%m-%d"))
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value='Jawa Barat')
df.insert(loc=3, column='kabkot', value = 'Cirebon')
df.insert(loc=16, column='source_link', value='http://covid19.cirebonkab.go.id/')
df.insert(loc=17, column='types', value='kabkot')
df.insert(loc=18, column='user_pic', value='Dea')
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
        print(date + " Kabupaten Cirebon Done")
        
import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')