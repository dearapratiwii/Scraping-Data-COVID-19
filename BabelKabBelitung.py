from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime as dt
import time
import warnings
warnings.filterwarnings("ignore")

option = webdriver.ChromeOptions()
option.add_argument('--headless')
option.add_argument('--no-sandbox')
option.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver', options = option)
source = 'https://covid19.belitung.go.id/'
driver.get(source)
time.sleep(5)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

s = soup.find_all('strong')
s = [i.get_text().replace('2021,', '2021', 1) for i in s]
d = s[14].split()[3:6]
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(d[1].lower())+1
date_update = d[2]+'-'+str(bln)+'-'+d[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")
driver.quit()

data = {'total_positif':[s[9]],
       'positif_sembuh':[s[5]],
       'positif_dirawat':[s[1]],
        'positif_isolasi':[s[6]],
       'positif_meninggal':[s[7]],} 
df = pd.DataFrame(data) 
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value ='Kepulauan Bangka Belitung')
df.insert(loc=3, column='kabkot', value = 'Belitung')
df.insert(loc=9, column='source_link', value = source)
df.insert(loc=10, column='types', value = 'kabkot')
df.insert(loc=11, column='user_pic', value ='Dea')
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
        print(date + " Kabupaten Belitung Done")