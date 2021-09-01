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

"""
option = webdriver.ChromeOptions()
option.add_argument('--headless')
option.add_argument('--no-sandbox')
option.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver', options = option)
source= 'https://bangkatengahkab.go.id/kategori/detail/covid19'
driver.get(source)
"""

"""
content = driver.page_source
soup = BeautifulSoup(content,'lxml')
h3 = soup.find_all('h3', class_ = 'entry-title td-module-title')
"""

"""
a = soup.find_all('a')[175]
link = a['href']
link

driver.get(link)
content = driver.page_source
soup = BeautifulSoup(content,'lxml')
"""

"""
i = soup.find_all('img')[4]
link_image = i['src']
link_image
"""
#img = Image.open(requests.get(link_image, stream=True).raw) #error

#img = Image.open(requests.get(link_image, stream=True).raw)
img = cv2.imread(cv2.samples.findFile("corona/All/0Shell_script/assets/0bangkateng.jpg"))
#img = cv2.imread(cv2.samples.findFile("0bangkateng.jpg"))
#gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
#canny = cv2.Canny(gray, 50, 150)
cv2_imshow(img)

i1 = img[331:388, 71:265] #Total Positif
i2 = img[648:700, 920:1054] #Positif Sembuh
i3 = img[459:523, 896:1050] #Positif Dirawat
i4 = img[870:920, 140:250] #Positif Meninggal
i5 = img[299:360, 1165:1450] # Tanggal

im = [i1, i2, i3, i4, i5]
result = []
for i in range(len(im)):
    opencvImage = cv2.cvtColor(np.array(im[i]), cv2.COLOR_RGB2BGR)
    gray = cv2.cvtColor(opencvImage, cv2.COLOR_BGR2GRAY)
    #sharpen = cv2.filter2D(gray,-1,sharpenFilter())
    #img = cv2.threshold(sharpen, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
    rz = cv2.resize(gray, None, fx=1.5, fy=1.5, interpolation=cv2.INTER_CUBIC)
    read = pytesseract.image_to_string(rz,config=r'--oem 3 --psm 6')
    read = read.split("|")[0:]
    result.append(read)
hasil = []
for sublist in result:
    for item in sublist:
        hasil.append(item)
hasil = [i.replace('\n\x0c', '', 1).replace('”', '', 1) for i in hasil]

d = hasil[-1].split()
bulan = ['januari', 'februari', 'maret', 'april', 'mei', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(d[1].lower())+1
date_update = d[2]+'-'+str(bln)+'-'+d[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")

data = {'total_positif':[hasil[0]],
       'positif_sembuh':[hasil[1]],
       'positif_dirawat':[hasil[2]],
        'positif_meninggal':[hasil[3]]} 
df = pd.DataFrame(data) 
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value ='Kepulauan Bangka Belitung')
df.insert(loc=3, column='kabkot', value = 'Bangka Tengah')
df.insert(loc=8, column='source_link', value = source)
df.insert(loc=9, column='types', value = 'kabkot')
df.insert(loc=10, column='user_pic', value ='Dea')
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
        print(date + " Kabupaten Bangka Tengah Done")
