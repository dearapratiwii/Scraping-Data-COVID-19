from PIL import Image
import requests
from selenium import webdriver
import easyocr
import pandas as pd
from datetime import datetime as dt
import time

option = webdriver.ChromeOptions()
option.add_argument('--headless')
option.add_argument('--no-sandbox')
option.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome('chromedriver', options = option)
source = "https://lawancorona.batam.go.id/"
driver.get(source)

imgsrc = driver.find_element_by_xpath('//*[@id="page-2414"]/div/section/div/div/div/div/section[3]/div/div/div/div/div/div/div/div/div[1]/div[16]/figure/img')
x = imgsrc.get_attribute("src")

im = Image.open(requests.get(x, stream=True).raw)
im

# Total Positif
pstv_left = 372  
pstv_top = 135
pstv_right = 453 
pstv_bottom = 167
positif = im.crop((pstv_left, pstv_top, pstv_right, pstv_bottom))
positif

# Total Sembuh
sbh_left = 372  
sbh_top = 245
sbh_right = 453 
sbh_bottom = 276
sbh = im.crop((sbh_left, sbh_top, sbh_right, sbh_bottom))
sbh

# Positif Meninggal
mgl_left = 372  
mgl_top = 355
mgl_right = 453 
mgl_bottom = 388
mgl = im.crop((mgl_left, mgl_top, mgl_right, mgl_bottom))
mgl

# Positif Dirawat
rwt_left = 372  
rwt_top = 466
rwt_right = 453 
rwt_bottom = 496
rwt = im.crop((rwt_left, rwt_top, rwt_right, rwt_bottom))
rwt

# Total OTG
otg_left = 893  
otg_top = 213
otg_right = 917
otg_bottom = 230
otg = im.crop((otg_left, otg_top, otg_right, otg_bottom))
otg

# Positif Sembuh
psbh_left = 893  
psbh_top = 368
psbh_right = 918
psbh_bottom = 384
psbh = im.crop((psbh_left, psbh_top, psbh_right, psbh_bottom))
psbh

# Tanggal Update
tgl_left = 470  
tgl_top = 47
tgl_right = 550
tgl_bottom = 63
tgl = im.crop((tgl_left, tgl_top, tgl_right, tgl_bottom))
tgl

positif = positif.save("positif_btm.jpg")
sbh = sbh.save("sbh_btm.jpg")
mgl = mgl.save("mgl_btm.jpg")
rwt = rwt.save("rwt_btm.jpg")
otg = otg.save("otg_btm.jpg")
psbh = psbh.save("psbh_btm.jpg")
tgl = tgl.save("tgl_btm.jpg")

list_of_image = ["positif_btm.jpg","sbh_btm.jpg","mgl_btm.jpg","rwt_btm.jpg","otg_btm.jpg","psbh_btm.jpg","tgl_btm.jpg"]

from PIL import ImageEnhance
for j in range(len(list_of_image)):
    column = Image.open(list_of_image[j])
    gray = column.convert('L')
    #threshold = 200
    #im = gray.point(lambda p: p > threshold and 255)
    #enhancer = ImageEnhance.Contrast(gray)
    #im = enhancer.enhance(6)
    #e = ImageEnhance.Sharpness(im)
    #sh = e.enhance(6)
    blackwhite = gray.point(lambda x: 0 if x < 200 else 255, '1')
    blackwhite.save(list_of_image[j])
    
reader = easyocr.Reader(['id','en'], gpu=False)
result_1 = reader.readtext("positif_btm.jpg")
result_2 = reader.readtext("sbh_btm.jpg")
result_3 = reader.readtext("mgl_btm.jpg")
result_4 = reader.readtext("rwt_btm.jpg")
results_5 = reader.readtext("otg_btm.jpg")
results_6 = reader.readtext("psbh_btm.jpg")
results_7 = reader.readtext("tgl_btm.jpg")

total_positif = int(result_1[0][1])
total_sembuh = int(result_2[0][1])
positif_meninggal = int(result_3[0][1])
positif_dirawat = int(result_4[0][1])
tgl = results_7[0][1].split()

bulan = ['januari', 'februari', 'maret', 'april', 'mai', 'juni','juli','agustus', 'september', 'oktober','november','desember']
bln = bulan.index(tgl[1].lower())+1
date_update = tgl[2]+'-'+str(bln)+'-'+tgl[0]
date_update = dt.strptime(date_update,'%Y-%m-%d').strftime('%Y-%m-%d')
date = dt.now().strftime("%Y-%m-%d")
date_update

data = {'total_positif':[total_positif],
       'positif_isolasi':[total_positif - total_sembuh - positif_dirawat],
       'positif_dirawat':[positif_dirawat],
        'positif_meninggal':[positif_meninggal]} 
df = pd.DataFrame(data)
df.insert(loc=0, column='scrape_date', value = date)
df.insert(loc=1, column='date_update', value = date_update)
df.insert(loc=2, column='provinsi', value ='Kepulauan Riau')
df.insert(loc=3, column='kabkot', value = 'Kota Batam')
df.insert(loc=8, column='source_link', value = source)
df.insert(loc=9, column='types', value = 'kabkot')
df.insert(loc=10, column='user_pic', value ='Dea')
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
        print(date + " Kota Batam Done")