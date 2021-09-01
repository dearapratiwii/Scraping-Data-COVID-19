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
source = 'https://covid19.mukomukokab.go.id/covid/'
driver.get(source)
time.sleep(30)

content = driver.page_source
soup = BeautifulSoup(content, 'lxml')

h2 = soup.find_all('h2', class_ = 'elementor-heading-title elementor-size-default')[1].get_text().split()[2:3]
date_update = dt.strptime(h2[0],'%d-%m-%Y').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

div = soup.find_all('span', class_ = 'elementor-counter-number')
links = str(div)
links = links.split()
a = list(filter(lambda k: 'data-to-value=' in k, links))
p = [i.replace('data-to-value="', '',1) for i in a]
p = [i.replace('">0</span>,', '',1) for i in p]
p = [i.replace('">0</span>]', '',1) for i in p]
for i in range(0, len(p)): 
    p[i] = int(p[i])

data = {'positif_sembuh':[p[1]],
        'positif_meninggal':[p[2]],
        'positif_dirawat':[p[6]],
        'total_positif':[p[7]],} 
df = pd.DataFrame(data)

df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value='Bengkulu')
df.insert(loc=3, column='kabkot', value = 'Mukomuko')
df.insert(loc=8, column='source_link', value='https://covid19.mukomukokab.go.id/covid/')
df.insert(loc=9, column='types', value='kabkot')
df.insert(loc=10, column='user_pic', value='Dea')
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
        print(date + ' Kabupaten Mukomuko Done')

import pymysql.cursors
import sqlalchemy

engine = sqlalchemy.create_engine('mysql+pymysql://covid_user5bb6593aa078@db-blanja2:3306/covid')
