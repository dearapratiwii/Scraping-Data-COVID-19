from selenium import webdriver
import pandas as pd
from datetime import datetime as dt
import time

source = "https://covid19.muarojambikab.go.id/home/infografis_detail/53"
date = dt.now().strftime("%Y-%m-%d")
tgl = "04-09-2020"
date_update = dt.strptime(tgl,'%d-%m-%Y').strftime('%Y-%m-%d')

df = pd.read_csv("/home/probis/notebook/corona/All/Dea/Manual/Muaro Jambi.csv", sep = ';')
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value ='Jambi')
df.insert(loc=3, column='kabkot', value = 'Muaro Jambi')
df.insert(loc=9, column='source_link', value = source)
df.insert(loc=10, column='types', value = 'kecamatan')
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
        print(date + " Kabupaten Muaro Jambi Done")