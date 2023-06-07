import json #for json files
import sys #used to close json file
import asf_search as asf #ASF search API tool
from hyp3_sdk import HyP3 #ASF API tool for sending INSAR jobs
import csv #creating a .csv
import requests #for checking status of job

#import libraries for file management
import os 
from zipfile import ZipFile 
import shutil

import geopandas as gpd
import pandas as pd
from datetime import datetime
from datetime import timedelta

#Libraries for dealing with geotifs
from osgeo import gdal,ogr
import rasterio as rio

#Math and plotting
import numpy as np
from scipy.ndimage import maximum_filter
import earthpy.plot as ep
import matplotlib.pyplot as plt
import psycopg2

import subprocess
from mintpy.cli import tsview
from mintpy.cli import plot_transection
import ipympl

#Takes a shapeFile (Site), a json file(login), and the database connection
def get_Disp_Maps(Site,login,conn):
    #First get the geometry of the shape file to use as the Polygon parameter for the ASF search
    geometry = str(Site['geometry'][0])
    date = str(Site['Date'][0])
    JobName = str(Site['Name'][0]).replace(" ","").replace("-","")
    d2 = datetime.strptime(date, '%Y-%m-%d')
    end = str(d2 + timedelta(days = 1)*365) #creating the end date for job collection
    start = str(d2 - timedelta(days = 1)*365) #creating the start date for job collection

    #Getting the username and password from the json file
    open = open(login)
    login = json.load(open)
    open = None

    userName = login['UserName']
    if userName == "":
        print("Please input Username")
        sys.exit()
    pw = login['Password']
    if pw =="":
        print("Please input Password")
        sys.exit()

    #Create an API search based on set parameters
    try:
        searchResults = asf.search(
            platform = "Sentinel-1",
            beamMode = "IW",
            polarization = "VV+VH",
            intersectsWith = geometry, 
            processingLevel= "SLC",
            start = start,
            end = end
        )
    except:
        pass

    data = searchResults.geojson()

    #Extracts scenes from job search and creates list of all scene names
    def get_scene_name(): 
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
    scenesList= get_scene_name() #list with all scene names

    filtered = scenesList.drop_duplicates()
    scenes = list(filtered['SceneNames'])
    dates = list(filtered['Dates'])

    def create_jobs(data,date): 
        jobs = [] 
        count = len(data) 
        for i in range(count-1): 
            scene1 = data[i]
            scene2 = data[i+1]
            if date[i+1].endswith('Z'):
                sceneDate = datetime.strptime(date[i+1], '%Y-%m-%dT%H:%M:%S.%fZ').date()
            else:
                sceneDate = datetime.strptime(date[i+1], '%Y-%m-%dT%H:%M:%S.%f').date()
            print(sceneDate)
            print(len(scene2))
            insar_job = HyP3.prepare_insar_job(scene1, scene2, name = JobName, include_displacement_maps=True,include_dem=True,include_look_vectors=True) 
            print(insar_job)
            jobs.append(insar_job) 
            pair = scene1+','+scene2
            try:
                cursor = conn.cursor()
                cursor.execute("INSERT INTO project1 (pair, site, reference_date) VALUES(%s, %s, %s)", (pair, JobName, sceneDate))
                conn.commit()
                cursor.close()
            except:
                conn = psycopg2.connect(host='localhost',database = 'postgres', user='postgres',password = 'postgrespw',port=32768)
        return jobs 
    jobsList = create_jobs(scenes, dates)

    hyp3 = HyP3(username = userName, password = pw) #authenticate using ASF credentials
    batch = hyp3.submit_prepared_jobs(prepared_jobs = jobsList)

    #Creates New Folder called "data" where the jobs will be downloaded
    directory = os.getcwd()
    output = os.path.join(directory, 'rawData')
    if not os.path.exists(output):
        os.makedirs(output)

    #Watch and Download job
    if not batch.complete():
        batch = hyp3.watch(batch)
    #Downloads files into data folder
    batch.download_files(location = output)

    #Creates List of zip file names
    zipfiles = os.listdir(output)

    #Unzips each zip file and deletes the zip
    for x in range(len(zipfiles)):
        file = os.path.join(output, zipfiles[x])
        print(file)
        with ZipFile(file, 'r') as zip:
            zip.extractall(output)
        os.remove(file)

#Give function path to directory
def crop_InSAR(directory):
    output = os.path.join(directory, 'rawData')
    folderList = os.listdir(output)
    tifList = []
    #Loop through the folders
    for x in range(len(folderList)):
        folderName = os.path.join(output, folderList[x])
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
                    cursor.execute("SELECT insar_name from project1 where insar_name = %s",(folderList[x],))
                    row = cursor.fetchall()
                    if row == []:
                        cursor.execute("UPDATE project1 SET insar_name = %s where pair = %s",(folderList[x],pair))
                        conn.commit()
                    cursor.close()
                except:
                    conn = psycopg2.connect(host='localhost',database = 'postgres', user='postgres',password = 'postgrespw',port=32768)

    dataset = rio.open(tifList[0])
    boundingBox = dataset.bounds
    left = boundingBox.left
    right = boundingBox.right
    top = boundingBox.top
    bottom = boundingBox.bottom
    print(boundingBox)
    for x in range(1,len(tifList)):
        dataset = rio.open(tifList[x])
        boundingBox = dataset.bounds
        if boundingBox.left < left:
            left = boundingBox.left
        if boundingBox.right < right:
            right = boundingBox.right
        if boundingBox.top < top:
            top = boundingBox.top
        if boundingBox.bottom < bottom:
            bottom = boundingBox.bottom
        print(left,bottom,right,top)
    minbbox = (left,bottom,right,top)

    #Crop the water mask incase it does not have the min bounds
    #For loop to crop all the tif files to the min bounds
    #all files need to be the same height and width to work with them in numpy
    opts = gdal.WarpOptions(options=['tr'], outputBounds=minbbox, format="GTiff")
    for geoTif in tifList:
        dataset = gdal.Open(geoTif)
        newfile = gdal.Warp(geoTif.split('.')[0]+'_crop.tif', dataset , options=opts)
        dataset = None #close the file

database = psycopg2.connect(host='localhost',database = 'postgres', user='postgres',password = 'postgrespw',port=32768)