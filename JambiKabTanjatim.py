from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime as dt
import requests
from PIL import Image, ImageCms
import pytesseract 
import cv2   
import os 
import io
from google.colab.patches import cv2_imshow
import warnings
warnings.filterwarnings("ignore")

option = webdriver.ChromeOptions()
option.add_argument('--headless')
option.add_argument('--no-sandbox')
option.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver', options = option)
source= 'https://tanjabtimkab.go.id/'
driver.get(source)
content = driver.page_source
soup = BeautifulSoup(content,'lxml')

i = soup.find_all('img')[-2]
link_image = i['src']
img = Image.open(requests.get(link_image, stream=True).raw)
img

# left, top, right, bottom
i1 = img.crop((693, 498, 782, 550)) #Positif Meninggal
i2 = img.crop((242, 493, 285, 520)) #Total Positif
i3 = img.crop((292, 518, 310, 534)) #Positif Dirawat
i4 = img.crop((96, 493, 135, 518)) #Total_pdp
i5 = img.crop((139, 520, 158, 536)) #PDP_isolasi
i6 = img.crop((157, 98, 281, 122)) #tanggal

result=[]
opencvImage = cv2.cvtColor(np.array(i3), cv2.COLOR_RGB2BGR)
gray = cv2.cvtColor(opencvImage, cv2.COLOR_BGR2GRAY)
rz = cv2.resize(gray, None, fx=6, fy=6, interpolation=cv2.INTER_CUBIC)
read = pytesseract.image_to_string(rz, config=r'--oem 3 --psm 6')
result.append(read)

opencvImage = cv2.cvtColor(np.array(i5), cv2.COLOR_RGB2BGR)
gray = cv2.cvtColor(opencvImage, cv2.COLOR_BGR2GRAY)
rz = cv2.resize(gray, None, fx=4, fy=4, interpolation=cv2.INTER_CUBIC)
read = pytesseract.image_to_string(rz, config=r'--oem 3 --psm 6')
result.append(read)

im = [i1, i2, i4, i6]
      #, i7, i8, i9, i10,i11, i12]
for i in range(len(im)):
    opencvImage = cv2.cvtColor(np.array(im[i]), cv2.COLOR_RGB2BGR)
    gray = cv2.cvtColor(opencvImage, cv2.COLOR_BGR2GRAY)
    #sharpen = cv2.filter2D(gray,-1,sharpenFilter())
    #img = cv2.threshold(sharpen, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
    #rz = cv2.resize(gray, None, fx=6, fy=6, interpolation=cv2.INTER_CUBIC)
    read = pytesseract.image_to_string(gray,config=r'--oem 3 --psm 6')
    result.append(read)
hasil = [i.replace('\n\x0c', '', 1).replace('Orang', '', 1) for i in result]
hasil

d = hasil[-1].replace('2071', '2021', 1).split()
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(d[1].lower())+1
date_update = d[2]+'-'+str(bln)+'-'+d[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

data = {'total_positif':[hasil[3]],
       'positif_meninggal':[hasil[2]],
       'positif_dirawat':[hasil[0]],
        'total_pdp':[hasil[4]],
       'pdp_isolasi':[hasil[1]],} 
df = pd.DataFrame(data) 
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value ='Jambi')
df.insert(loc=3, column='kabkot', value = 'Tanjung Jabung Timur')
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
        print(date + " Kabupaten Tanjung Jabung Timur Done")
