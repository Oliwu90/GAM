#import libraries
import requests
import datetime
import numpy as np
import pandas as pd
import os
import sys

#import CUSTOMED libraries
import NOAA_functions as NOAA

#get token from (https://www.ncdc.noaa.gov/cdo-web/token)
mytoken = 'rwRpetCadTrcOwhwVpXZOaXulnVXUlzr'





#Use the datetime package to get 14 days data
range=14
days_ago = datetime.datetime.now()-datetime.timedelta(days=range)

#Use the same begin and end date for just one day's data. Format for the API request
begin_date = days_ago.strftime("%Y-%m-%d")
end_date = datetime.datetime.now().strftime("%Y-%m-%d")

#Location key for the region you are interested in (can be found on NOAA or requested as a different API as well)
locationid = 'FIPS:42' #location id
# locationid = '' #location id
datasetid = 'GHCND' #datset id for "Daily Summaries"


#prepare endpoints
base_url_data = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data'
base_url_stations = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/stations'

df_weather = NOAA.get_weather(locationid, datasetid, begin_date, end_date, mytoken, base_url_data)
df_weather.head()
print('\n')
df_stations = NOAA.get_station_info(locationid, datasetid, mytoken, base_url_stations)
df_stations.head()
print('\n')

#Merge to df 
df = df_weather.merge(df_stations, left_on = 'station', right_on = 'id', how='inner')

#Check for missing overlap between station weather info and location info
    
location_ismissing = df_weather[~df_weather['station'].isin(df_stations['id'])]
loc_miss_count = len(location_ismissing['station'].unique())
if loc_miss_count != 0:
    print("Missing location data for "+str(loc_miss_count)+" stations")
else:
    print("Successfully retrieved and combined location data")

#remove id, masdate, midate column
df.drop('id',inplace=True,axis=1)
df.drop(['maxdate','mindate'],inplace=True,axis=1)
df.drop(['datacoverage','elevation'],inplace=True,axis=1)

df.to_csv('NOAA_'+str(datetime.datetime.now().strftime("%Y%m%d"))+'_historical '+str(range)+' days data'+'.csv', encoding='utf-8', index=False)
