#import packages used
import os 
from datetime import datetime
from datetime import timedelta
import statistics as stats
import asf_search as asf
import pandas as pd
from hyp3_sdk import HyP3 
import geopandas as gpd
from zipfile import ZipFile 
from osgeo import gdal,ogr
import rasterio as rio
import rioxarray as rxr
import rasterstats as rs
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from dateutil.relativedelta import relativedelta
import subprocess
import matplotlib.patches as mpatches
import matplotlib.ticker as ticker


#Extracts scenes from ASF job search and creates list of all scene names, dates, frames, and paths. Exports the list as a csv
#data:geojson
def get_scene_name(data,name): 
    count = len(data["features"])                   
    scenes = []
    dates = []
    frames=[]
    paths=[]
    urls = []
    for x in range(count):
        scene = data["features"][x]["properties"]["sceneName"]
        date = data["features"][x]["properties"]["startTime"]
        frame = data["features"][x]["properties"]["frameNumber"]
        path = data["features"][x]["properties"]["pathNumber"]
        url = data["features"][x]["properties"]["url"]
        scenes.append(scene)
        dates.append(date)
        frames.append(frame)
        paths.append(path)
        urls.append(url)
    df = pd.DataFrame(list(zip(dates, scenes, frames, paths, urls)),columns =['Dates', 'SceneNames','Frames', 'Paths','Urls'])
    #Sort by Path then frame then date
    df.sort_values(by=['Paths','Frames','Dates'],ignore_index=True,inplace=True)
    dfDropDup=df.drop_duplicates()
    dfDropDup.to_csv(name+'.csv')
    return dfDropDup

#creates the list of all jobs to be sent to ASF servers and inputs the scene pairs for each site into the database   
#site:str (site name in gpd), data:list (list of scenes), date:list (list of dates), conn: connection to db, tname:str (project table name)
def create_jobs(site,data,conn,tName): 
    listjobs=[]
    count = len(data) 
    
    #for all the scenes in the scene list we will pair the scenes together to create InSARs
    for i in range(count-1): 
        
        #We want to create triplets for all the SARs images so another loop is needed to create triplets:
        for j in range(1, 4):
            if i +j >= count:
                break
            
            #The path and the frame of the two InSARs need to be the same to sucessfully create an InSAR of the pair.
            #This checks to make sure the paths and frames are the same:
            if data['Paths'][i] != data['Paths'][i+j] or data['Frames'][i] != data['Frames'][i+j]:
                continue
            
            #If path and frame are equal create the pair
            scene1 = data['SceneNames'][i]
            scene2 = data['SceneNames'][i+j]
            if data['Dates'][i+1].endswith('Z'):
                sceneDate = datetime.strptime(data['Dates'][i], '%Y-%m-%dT%H:%M:%S.%fZ').date(),datetime.strptime(data['Dates'][i+j], '%Y-%m-%dT%H:%M:%S.%fZ').date()
            else:
                sceneDate = datetime.strptime(data['Dates'][i], '%Y-%m-%dT%H:%M:%S.%f').date(),datetime.strptime(data['Dates'][i+j], '%Y-%m-%dT%H:%M:%S.%f').date()
            insar_job = HyP3.prepare_insar_job(scene1, scene2, name = tName, include_displacement_maps=True,include_dem=True,include_look_vectors=True)

            #Inputting the primary scene, secondary scene, aquisition dates for the two scenes, and the site name into the SQLlite database.
            try:
                cursor = conn.cursor()
                cursor.execute(f"SELECT primary_scene, secondary_scene, primary_date, secondary_date FROM {tName} WHERE primary_scene = ? AND secondary_scene = ?", (scene1,scene2)) #checking if the pair already exist in the database
                row = cursor.fetchall()
                #if pair is in db then add new row with same data, but with the new site
                if row != []: 
                    cursor.execute(f"INSERT INTO {tName} (primary_scene, secondary_scene, primary_date, secondary_date, site) VALUES (?, ?, ?, ?, ?)", (row[0][0], row[0][1],row[0][2],row[0][3], site))
                    conn.commit()
                    cursor.close()
                    #do not add to jobs list
                    
                #if pair is not in db insert all new dats into the database
                else:
                    cursor.execute(f"INSERT INTO {tName} (primary_scene, secondary_scene, site, primary_date, secondary_date) VALUES(?, ?, ?, ?, ?)", (scene1,scene2, site, sceneDate[0],sceneDate[1]))
                    #add to jobs list
                    listjobs.append(insar_job)
                    conn.commit()
                    cursor.close()
            except Exception as e:
                print(e)
    return listjobs 

#shpFile:str (path to .shp file), conn: connection to db, tname:str (project table name)
def insar_jobs(shpFile,conn,tName): 
    shp: gpd.GeoDataFrame = gpd.read_file(shpFile) #Open shapefile as a geopandas dataframe
    jobs=[]
    for site in range(len(shp)): #for each site in the shapefile
        dateSTR = shp['Date'][site] #Get the date of enrichment for the site
        siteName = str(shp['Name'][site]) #Get the name of the site
        date = datetime.strptime(dateSTR, '%Y-%m-%d')
        start = str(date - timedelta(weeks = 52)) #1 year pre-enrichment
        end = str(date + timedelta(weeks = 52)) #1 year post enrichment
        geo = str(shp['geometry'][0]) #get the geometry of the site
        print(shp.crs)
        print("-------")
        print(geo)
        print("-------")
        
        #Create an API search based on set parameters:
        searchResults = asf.search(platform = "Sentinel-1", beamMode = "IW", polarization = "VV+VH", intersectsWith = geo, processingLevel= "SLC", start = start, end = end)
        searchJson = searchResults.geojson()
        scenesList= get_scene_name(searchJson,siteName) #call the get_scene_name function
        #print(scenesList)
        jobsList = create_jobs(siteName,scenesList,conn,tName) #call the create_jobs function
        jobs.extend(jobsList)
    print("You will send "+str(len(jobs))+" jobs to ASF.")
    return jobs #returns the prepared jobs that cane be sent to ASF

#sends the list of jobs created in  insar_jobs functions to ASF to be created and downloaded
#UN:str (ASF username), PW:str (ASF password), jobs:list (list of ASF jobs), out:str (Path for save data)
def send_jobs(UN,PW,jobs,out): 
    hyp3 = HyP3(username = UN, password = PW) #authenticate using ASF credentials
    batch = hyp3.submit_prepared_jobs(prepared_jobs = jobs) #send the jobs

    if not batch.complete():
        batch = hyp3.watch(batch)
    #Downloads files into data folder
    batch.download_files(location = out)

#unzip the downloaded files from ASF and delete the zipped files
def unzip(zipped):
    zipfiles = os.listdir(zipped)
    #Unzips each zip file and deletes the zip
    for x in range(len(zipfiles)):
        file = os.path.join(zipped, zipfiles[x])
        if file.endswith(".zip"):
            with ZipFile(file, 'r') as zipf:
                zipf.extractall(zipped)
            os.remove(file)

#Gets the minimum bounds of all the tif files in the files            
def get_bounds(listTifs):
    dataset = rio.open(listTifs[0])
    #set default bounds:
    boundingBox = dataset.bounds
    left = boundingBox.left
    right = boundingBox.right
    top = boundingBox.top
    bottom = boundingBox.bottom
    #loop through all tifs and check if they have smaller bounds
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
    minbbox = (left,bottom,right,top) #get all the min bounds
    print(minbbox)
    return minbbox

#reproject the tif files to be the same crs as the shapefile (uses gdal)
def reproject(newcrs,reprojTifs,conn,tName): #newcrs:str (crs of the shapefile), reprojTuf:list (list of all tif files), conn: connection to db, tname:str (project table name)
    opts = gdal.WarpOptions(options=['tr'], dstSRS=newcrs, format="GTiff") #set options for gdal
    #for all listed tifs:
    for geoTif in reprojTifs:
        dataset = gdal.Open(geoTif) #open the tif
        filename = geoTif.split('.')[0]+'_reproj.tif' #add reproj.tif to the end of the file name
        currentInsar = geoTif.split('\\')[-2]
        gdal.Warp(filename, dataset, options=opts) #reproject tif into new crs
        print(currentInsar)
        if filename.endswith("vert_disp_crop_reproj.tif"): #input path to vert displacement tifs into the db
            try: 
                cursor = conn.cursor()
                cursor.execute(f"UPDATE {tName} SET vertdisp_path = ? WHERE insar_name = ?",(filename, currentInsar))
                conn.commit()
                cursor.close()
            except Exception as e:
                print(e)

        if filename.endswith("_corr_crop_reproj.tif"): #input path to corr tif into the db
            try: 
                cursor = conn.cursor()
                cursor.execute(f"UPDATE {tName} SET coherence_path = ? WHERE insar_name = ?",(filename, currentInsar))
                conn.commit()
                cursor.close()
            except Exception as e:
                print(e)
        dataset = None #close the file

def delete_double(rawdatapath):
    folderList = os.listdir(rawdatapath) #List of all folders downloaded
    tifList = []
    #Loop through the folders
    for x in range(len(folderList)):
        folderName = os.path.join(rawdatapath, folderList[x])
        fileList = os.listdir(folderName) #list of all files in current folder
        #Loop through files in folder
        for file in fileList:
            #Select file that ends in vert_disp.tif
            if file.endswith("_crop.tif"):
                os.remove(os.path.join(folderName,file))

#crop the tifs to the minbouds from the get_bounds function (uses gdal)
def crop_Tifs(cropTifs): #cropTifs:list (list of all tif files)
    cropped = []
    minbbox = get_bounds(cropTifs) #call the get_bounds function returns list of minbounds
    opts = gdal.WarpOptions(options=['tr'], outputBounds=minbbox, format="GTiff") #set options for gdal
    #for all listed tifs:
    for geoTif in cropTifs:
        dataset = gdal.Open(geoTif) #open the tif
        filename = geoTif.split('.')[0]+'_crop.tif' #add crop.tif to the end of the file name

        gdal.Warp(filename, dataset, options=opts) #crop tifs to the min bounds
        cropped.append(filename) #append the new name of the cropped tif file to list
        dataset = None #close the file
    return(cropped) #return list of newly cropped tifs

#Takes all tifs downloaded from ASF crops and reprojects them so they can be used in annalysis
def process_tifs(rawdatapath,project,shpPath,conn):
    unzip(rawdatapath)   #calls the unzip function
    folderList = os.listdir(rawdatapath) #List of all folders downloaded
    tifList = []
    #Loop through the folders
    for x in range(len(folderList)):
        folderName = os.path.join(rawdatapath, folderList[x])
        fileList = os.listdir(folderName) #list of all files in current folder
        #Loop through files in folder
        for file in fileList:
            #Select file that ends in vert_disp.tif
            if file.endswith(".tif"):
                fileName = os.path.join(folderName, file)
                tifList.append(fileName)
            
            #Select text file to get the InSAR name for the paired scenes to input into db
            if file.startswith(folderList[x]+'.txt'):
                fileName = os.path.join(folderName, file)
                f = open(fileName,'r')
                lines = f.readlines()
                scene1 = lines[0].split(' ')
                scene2 = lines[1].split(' ')
            
                pair = scene1[2].replace('\n','')+','+scene2[2].replace('\n','')

                try:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT insar_name FROM {project} WHERE insar_name = ?",(folderList[x],))
                    row = cursor.fetchall()
                    if row == []:
                        cursor.execute(f"UPDATE {project} SET insar_name = ? WHERE primary_scene = ? AND secondary_scene = ?",(folderList[x], scene1, scene2))
                        conn.commit()
                    cursor.close()
                except Exception as e:
                    print(e)
    croppedList=crop_Tifs(tifList) #calls the crop_Tif function returns list of all cropped tifs
    shp = gpd.GeoDataFrame.from_file(shpPath)
    newcrs= str(shp.crs)
    reproject(newcrs,croppedList,conn,project) #calls the reproject function
    delete_double(rawdatapath)

#selects the insars from the db given a specific time frame and site
def get_insars(project,conn,date2,date1,shp,name):
    proj = gpd.GeoDataFrame.from_file(shp)
    for x in range(len(proj)):
        site = proj['Name'][x]
        if site == name:
            date = proj['Date'][x]
            break
    d2 = datetime.strptime(date, '%Y-%m-%d')
    print(d2)
    date2 = str(d2 + timedelta(weeks = 52))
    date1 = str(d2 - timedelta(weeks = 52))
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT vertdisp_path, reference_date from {project} where reference_date BETWEEN ? AND ? ORDER BY reference_date ASC",(date1,date2))
        insars = cursor.fetchall()
        cursor.close()
    except Exception as e:
        print(e)
    return insars

#calculates the zonal stats for 1 site
def get_zonal_stats(insars,site):
    timeseriesStats = gpd.GeoDataFrame(columns=['count','min','mean','max','median'])
    for x in range(len(insars)):
        openRaster = rxr.open_rasterio(insars[x])
        no_zeros = openRaster.where(openRaster != 0, np.nan)
        affine=openRaster.rio.transform()
        currentStats = rs.zonal_stats(site,
                                        insars[x],
                                        affine=affine,
                                        copy_properties=True,
                                        all_touched=True,
                                        stats="count min mean max median")
        properties=currentStats[0]
        stats = properties['count'], properties['min'], properties['mean'], properties['max'],properties['median']
        timeseriesStats.loc[len(timeseriesStats)] = stats
        openRaster=None
        affine=None
        no_zeros=None
    print(timeseriesStats['mean'])
    return timeseriesStats

def plot_mean(insars,shp,name):
    x=[]
    pathInSAR =[]
    for z in range(len(insars)):
        pathInSAR.append(insars[z][0])
        convertString = str(insars[z][1])
        convert = np.datetime64(convertString)
        x.append(convert)
    proj = gpd.GeoDataFrame.from_file(shp)
    for z in range(len(proj)):
        site = proj['Name'][z]
        if site == name:
            date = proj['Date'][z]
            index = proj['geometry'][z]
            break
    conDate = np.datetime64(date)
    d2 = datetime.strptime(date, '%Y-%m-%d')
    stats = get_zonal_stats(pathInSAR,index)
    fig, ax = plt.subplots(2, 2, figsize=(28, 10))
    plt.rcParams.update({'font.size': 30})
    #mean
    y = (stats['mean'])
    #y2 = np.cumsum(stats['mean'])
    ax=plt.subplot(1,1,1)
    ax.plot(x,y,color="b")
    vertDisp = mpatches.Patch(color='b', label='mean displacement')
    #ax.plot(x,y2,color="m")
    #cumSUM = mpatches.Patch(color='m', label='cumulative sum')
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(3,6,9,12)))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    line=plt.axvline(x = conDate, color = 'r', label = 'enrichment 7/2017')
    ax.grid(True)
    plt.xlabel('Date')
    plt.ylabel('Displacement (meters)')
    plt.grid(True)
    ax.legend(handles=[vertDisp,line])
    fig.suptitle(name)
    plt.show()
    return stats

def all_sites(project,conn,shp):
    proj = gpd.GeoDataFrame.from_file(shp)
    
    for sites in range(len(proj)):
        name = proj['Name'][sites]
        index = proj['geometry'][sites]
        print(name)
        date = proj['Date'][sites]
        d1 = datetime.strptime(date, '%Y-%m-%d')
        m=d1.month #month of enrichment
        d2 = d1 + relativedelta(months = 6) 
        m2 = d2.month
        conDate = np.datetime64(date)

        #Get all insars for the site
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT vertdisp_path, reference_date from {project} where site = ? ORDER BY reference_date ASC",(name,))
            insars = cursor.fetchall()
            cursor.close()
        except Exception as e:
            print(e)

        pathInSAR =[]
        x=[]
        for z in range(len(insars)):
            pathInSAR.append(insars[z][0])
            convertString = str(insars[z][1])
            convert = np.datetime64(convertString)
            x.append(convert)
        stats = get_zonal_stats(pathInSAR,index) #calls zonal stats function

        fig, ax = plt.subplots(2, 2, figsize=(10, 7))
        fig.tight_layout(h_pad=3.5,w_pad=3.5)

        #plot the mean
        y = (stats['mean'])
        ax=plt.subplot(2,2,1)
        ax.plot(x,y)
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(m, m2)))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
        plt.axvline(x = conDate, color = 'r', label = 'axvline - full height')
        ax.grid(True)
        plt.xlabel('Date')
        plt.ylabel('Displacement (meters)')
        plt.title('mean')
        plt.grid(True)
        
        #plot the median
        y = (stats['median'])
        ax = plt.subplot(2,2,2)
        ax.plot(x,y)
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(m, m2)))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
        plt.axvline(x = conDate, color = 'r', label = 'axvline - full height')
        plt.xlabel('Date')
        plt.ylabel('Displacement(meters)')
        plt.title('median')
        plt.grid(True)

        #plot the min
        y = (stats['min'])
        ax = plt.subplot(2,2,3)
        ax.plot(x,y)
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(m, m2)))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
        plt.axvline(x = conDate, color = 'r', label = 'axvline - full height')
        plt.xlabel('Date')
        plt.ylabel('Displacement(meters)')
        plt.title('min')
        plt.grid(True)

        #plot the max
        y = (stats['max'])
        ax = plt.subplot(2,2,4)
        ax.plot(x,y)
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(m, m2)))
        ax.xaxis.set_minor_locator(mdates.MonthLocator())
        plt.axvline(x = conDate, color = 'r', label = 'axvline - full height')
        plt.xlabel('Date')
        plt.ylabel('Displacement(meters)')
        plt.title('max')
        plt.grid(True)

        fig.suptitle(name)
        plt.subplots_adjust(top=0.90)
        plt.show()
        print(stats.mean(stats['count']))
'''
def run_mintpy():
    subprocess.run(["smallbaselineApp.py","mintpyConfigFile.txt"])
    subprocess.run(["smallbaselineApp.py", "--dostep", "velocity", "mintpyConfigFile.txt"])'''



#Testing GNSS mapping:
def parseGNSSData(path):
    #create dataframe of gps data
    textData = open(path,'r')
    lines = textData.readlines()
    textData.close()
    headers = lines[0].split() #get the headings from the text file
    lines.pop(0)
    x = []
    for line in lines:
        newLine=line.split()
        x.append(newLine)
    Df = pd.DataFrame(x, columns=headers) #dataframe created fromm gps
    return Df
    

def mapGNSS(project,conn,shp,name,gpsPath,data):
    gpsDf = parseGNSSData(gpsPath) #Call function to turn GNSS text file to a dataframe
    lat = float(gpsDf['_latitude(deg)'][0]) #latitude of the GNSS station
    lon = float(gpsDf['_longitude(deg)'][0]) #longitude if the GNSS station
    print(lat,lon)

    #Getting all InSARs for sites
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT DISTINCT vertdisp_path, reference_date from {project} ORDER BY reference_date ASC")
        insars = cursor.fetchall()
        cursor.close()
    except Exception as e:
        print(e)

    pathInSARAll =[]
    xAll=[]
    for z in range(len(insars)):
        pathInSARAll.append(insars[z][0])
        convertString = str(insars[z][1])
        convert = np.datetime64(convertString)
        xAll.append(convert)
   
    print('Checkpoint1')
   
    #Getting the InSARs for the specified site
    proj = gpd.GeoDataFrame.from_file(shp)
    for z in range(len(proj)):
        site = proj['Name'][z]
        if site == name:
            date = proj['Date'][z]
            index = proj['geometry'][z]
            break
    print(site)
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT vertdisp_path, reference_date from {project} where site = ? ORDER BY reference_date ASC",(name,))
        insars = cursor.fetchall()
        cursor.close()
    except Exception as e:
        print(e)
       
    pathInSARSite =[]
    xSite=[]
    for z in range(len(insars)):
        pathInSARSite.append(insars[z][0])
        convertString = str(insars[z][1])
        convert = np.datetime64(convertString)
        xSite.append(convert)

    stats = get_zonal_stats(pathInSARSite,index) #calls zonal stats function
    conDate = np.datetime64(date)
    print('Checkpoint2')
    #Get a timeseries of the point at lat,lon of GNSS station
    dispTS =[]
    for tif in pathInSARAll:
        openraster =gdal.Open(tif)
        transform = openraster.GetGeoTransform()
        xOrigin = transform[0]
        yOrigin = transform[3]
        pixelWidth = transform[1]
        pixelHeight = transform[5]

        band = openraster.GetRasterBand(1)
        data = band.ReadAsArray()
        openraster=None
        xOffset = int((lon - xOrigin) / pixelWidth)
        yOffset = int((lat - yOrigin) / pixelHeight)
        # get individual pixel values
        value = data[yOffset][xOffset]
        dispTS.append(value) #timeseries for the one point

    #get timeseries for gnss data
    start = np.datetime64(xAll[0])
    end = np.datetime64(xAll[-1])

    gpsList=[]
 
    for i in range(len(gpsDf)-1):
        dateAll = datetime.strptime(gpsDf['YYMMMDD'][i+1], '%y%b%d')
        convertAll = np.datetime64(dateAll)
        diff = float(gpsDf['__height(m)'][i]) - float(gpsDf['__height(m)'][i+1])
        if convertAll >= start and convertAll <= end:
            row = [convertAll,diff]
            gpsList.append(row)

    gpsData = pd.DataFrame(gpsList,columns=['Date','Height'])
    GNSSDisp = [gpsData['Height'][0]]
    Sum = 0 
    j = 1
    print(gpsData['Date'][0],xAll[0])
    for i in range(1,len(gpsData)):
        print(gpsData['Date'][i],xAll[j])
        if gpsData['Date'][i] != xAll[j]:
            Sum = Sum + gpsData['Height'][i]
        else:
            GNSSDisp.append(Sum)
            Sum=0
            j=j+1
    print(GNSSDisp)
    print(xAll)
    print(len(xAll),len(GNSSDisp))
    print('Checkpoint3')

    #plotting
    fig, ax = plt.subplots(2, 2, figsize=(28,18))
    plt.rcParams.update({'font.size': 30})
    y = (dispTS)
    y2 = (gpsData['Height'])
    #y3 = stats['mean']
    print('Pixel:',max(dispTS),min(dispTS),'GPS:',max(gpsData['Height']),min(gpsData['Height']),'Mean:',max(stats['mean']),min(stats['mean']))
    ax=plt.subplot(1,1,1)
    ax.plot(xAll,y,color="b")
    vertDisp = mpatches.Patch(color='b', label='Pixel Timeseries')
    ax.plot(gpsData['Date'],y2,color="m")
    gpsTimeSeries = mpatches.Patch(color='m', label='GPS Timeseries')
    #ax.plot(xSite,y3,color="g")
    #siteTimeSeries = mpatches.Patch(color='g', label='MCA1 Mean')
    ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(3,6,9,12)))
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    #yaxis = [max(gpsData['Height']),min(dispTS)]
    #tick_spacing = 1
    #ax.yaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
    ax.grid(True)
    plt.xlabel('Date')
    plt.ylabel('Height (meters)')
    plt.grid(True)
    ax.legend(handles=[vertDisp,gpsTimeSeries])
    fig.suptitle(name)
    plt.show()
 