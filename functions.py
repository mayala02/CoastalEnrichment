
import os 
import psycopg2

from datetime import datetime
from datetime import timedelta
import asf_search as asf #ASF search API tool
import pandas as pd
from hyp3_sdk import HyP3 #ASF API tool for sending INSAR jobs
import geopandas as gpd
import time

from zipfile import ZipFile 
from osgeo import gdal,ogr
import rasterio as rio

#Extracts scenes from job search and creates list of all scene names
def get_scene_name(data): 
    count = len(data["features"])                   
    scenes = []
    dates = []
    for x in range(count):
        scene = data["features"][x]["properties"]["sceneName"]
        date = data["features"][x]["properties"]["startTime"]
        scenes.append(scene)
        dates.append(date)
    scenes.reverse()
    dates.reverse()
    df = pd.DataFrame(list(zip(dates, scenes)),columns =['Dates', 'SceneNames'])
    return df
    
def create_jobs(site,data,date,conn,tableName): 
    listjobs=[]
    count = len(data) 
    for i in range(count-1): 
        scene1 = data[i]
        scene2 = data[i+1]
        if date[i+1].endswith('Z'):
            sceneDate = datetime.strptime(date[i+1], '%Y-%m-%dT%H:%M:%S.%fZ').date()
        else:
            sceneDate = datetime.strptime(date[i+1], '%Y-%m-%dT%H:%M:%S.%f').date()
        insar_job = HyP3.prepare_insar_job(scene1, scene2, name = tableName, include_displacement_maps=True,include_dem=True,include_look_vectors=True)
        pair = scene1+','+scene2
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT pair, reference_date FROM {tableName} WHERE pair = %s", (pair,))
            row = cursor.fetchall()
            #if pair is in db
            if row != []:
                #then add new row with same data, but use the JobName as the site
                cursor.execute(f"INSERT INTO {tableName} (pair, reference_date, site) VALUES (%s, %s, %s)", (row[0], row[1], site))
                conn.commit()
                cursor.close()
                #do not add to jobs list
            else:
                #just do the insert as is
                cursor.execute(f"INSERT INTO {tableName} (pair, site, reference_date) VALUES(%s, %s, %s)", (pair, site, sceneDate))
                #add to jobs list
                listjobs.append(insar_job)
                conn.commit()
                cursor.close()
        except Exception as e:
            print(e)
            conn = psycopg2.connect(host='localhost',database = 'postgres', user='postgres',password = 'postgrespw',port=32768)
    return listjobs 

def insar_jobs(shpFile,conn,tname):
    shp = gpd.GeoDataFrame.from_file(shpFile)
    jobs=[]
    for site in range(len(shp)):
        date = str(shp['Date'][site])
        siteName = str(shp['Name'][site]).replace(" ","").replace("-","")
        dateOG = datetime.strptime(date, '%Y-%m-%d')
        start = str(dateOG - timedelta(weeks = 52))
        end = str(dateOG + timedelta(weeks = 52))
        geo = str(shp['geometry'][0])
        #Create an API search based on set parameters
        searchResults = asf.search(platform = "Sentinel-1", beamMode = "IW", polarization = "VV+VH", intersectsWith = geo, processingLevel= "SLC", start = start, end = end)
        searchJson = searchResults.geojson()
        scenesList= get_scene_name(searchJson) #list with all scene names
        filtered = scenesList.drop_duplicates()
        scenes = list(filtered['SceneNames'])
        dates = list(filtered['Dates'])
        jobsList = create_jobs(siteName,scenes,dates,conn,tname)
        jobs.extend(jobsList)
        
    return jobs

def send_jobs(UN,PW,jobs,out):
    hyp3 = HyP3(username = UN, password = PW) #authenticate using ASF credentials
    batch = hyp3.submit_prepared_jobs(prepared_jobs = jobs)
    now = datetime.now()
    end = now + timedelta(hours = 5)
    #Watch and Download job
    while batch.complete() != True:
        time.sleep(600)
        now = datetime.now()
        if now >= end:
            print('TimedOut :(')
            break
    #Downloads files into data folder
    batch.download_files(location = out)

def unzip(zipped):
    zipfiles = os.listdir(zipped)
    #Unzips each zip file and deletes the zip
    for x in range(len(zipfiles)):
        file = os.path.join(zipped, zipfiles[x])
        if file.endswith(".zip"):
            with ZipFile(file, 'r') as zipf:
                zipf.extractall(zipped)
            os.remove(file)
            
def get_bounds(listTifs):
    dataset = rio.open(listTifs[0])
    boundingBox = dataset.bounds
    left = boundingBox.left
    right = boundingBox.right
    top = boundingBox.top
    bottom = boundingBox.bottom
    print(boundingBox)
    for x in range(1,len(listTifs)):
        dataset = rio.open(listTifs[x])
        boundingBox = dataset.bounds
        if boundingBox.left < left:
            left = boundingBox.left
        if boundingBox.right < right:
            right = boundingBox.right
        if boundingBox.top < top:
            top = boundingBox.top
        if boundingBox.bottom < bottom:
            bottom = boundingBox.bottom
    minbbox = (left,bottom,right,top)
    return(minbbox)

def crop_Tifs(uncroppedTifs,shpPath,project,conn):
    minbbox = get_bounds(uncroppedTifs)
    shp = gpd.GeoDataFrame.from_file(shpPath)
    newcrs= str(shp.crs)
    opts = gdal.WarpOptions(options=['tr'], outputBounds=minbbox, dstSRS=newcrs, format="GTiff")
    for geoTif in uncroppedTifs:
        dataset = gdal.Open(geoTif)
        filename = geoTif.split('.')[0]+'_crop.tif'
        currentInsar = geoTif.split('\\')[-2]
        newfile = gdal.Warp(filename, dataset, options=opts)
        
        if geoTif.endswith("vert_disp.tif"):
            try: 
                cursor = conn.cursor()
                cursor.execute(f"UPDATE {project} SET vertdisp_path = %s WHERE insar_name = %s",(filename, currentInsar))
                conn.commit()
                cursor.close()
            except Exception as e:
                print('1')
                print(e)
                conn = psycopg2.connect(host='localhost',database = 'postgres', user='postgres',password = 'postgrespw',port=32768)
        if geoTif.endswith("_corr.tif"):
            try: 
                cursor = conn.cursor()
                cursor.execute(f"UPDATE {project} SET coherence_path = %s WHERE insar_name = %s",(filename, currentInsar))
                conn.commit()
                cursor.close()
            except Exception as e:
                print(e)
                conn = psycopg2.connect(host='localhost',database = 'postgres', user='postgres',password = 'postgrespw',port=32768)
        dataset = None #close the file

def process_tifs(rawdatapath,project,shpPath,conn):
    unzipfiles = unzip(rawdatapath)  
    folderList = os.listdir(rawdatapath)
    tifList = []
    #Loop through the folders
    for x in range(len(folderList)):
        folderName = os.path.join(rawdatapath, folderList[x])
        fileList = os.listdir(folderName)
        #Loop through files in folder
        for file in fileList:
            #Select file that ends in vert_disp.tif
            if file.endswith(".tif"):
                fileName = os.path.join(folderName, file)
                tifList.append(fileName)
            if file.startswith(folderList[x]+'.txt'):
                fileName = os.path.join(folderName, file)
                f = open(fileName,'r')
                lines = f.readlines()
                scene1 = lines[0].split(' ')
                scene2 = lines[1].split(' ')
            
                pair = scene1[2].replace('\n','')+','+scene2[2].replace('\n','')
                try:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT insar_name FROM {project} WHERE insar_name = %s",(folderList[x],))
                    row = cursor.fetchall()
                    if row == []:
                        cursor.execute(f"UPDATE {project} SET insar_name = %s WHERE pair = %s",(folderList[x], pair))
                        conn.commit()
                    cursor.close()
                except Exception as e:
                    print(e)
                    conn = psycopg2.connect(host='localhost',database = 'postgres', user='postgres',password = 'postgrespw',port=32768)
    crop=crop_Tifs(tifList,shpPath,project,conn)