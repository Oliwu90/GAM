import requests
import numpy as np
import pandas as pd



'''
get specific data type based on location and data set id
'''
def get_weather(locationid, datasetid, begin_date, end_date, mytoken, base_url):
    token = {'token': mytoken}

    #passing as string instead of dict because NOAA API does not like percent encoding
    params = 'datasetid='+str(datasetid)+'&'+'locationid='+str(locationid)+'&'+'datatypeid=SNWD'+'&'+'startdate='+str(begin_date)+'&'+'enddate='+str(end_date)+'&'+'limit=1000'+'&'+'units=standard'
    
    r = requests.get(base_url, params = params, headers=token)
    print("Request status code: "+str(r.status_code))
    try:
        #results comes in json form. Convert to dataframe
        df = pd.DataFrame.from_dict(r.json()['results'])
        print("Successfully retrieved "+str(len(df['station'].unique()))+" stations")
        dates = pd.to_datetime(df['date'])
        print("Last date retrieved: "+str(dates.iloc[-1]))

        return df

    #Catch all exceptions for a bad request or missing data
    except:
        print("Error converting weather data to dataframe. Missing data?")


'''
get stations information 
'''

def get_station_info(locationid, datasetid, mytoken, base_url):
    token = {'token': mytoken}

    #passing as string instead of dict because NOAA API does not like percent encoding
    
    stations = 'locationid='+str(locationid)+'&'+'datasetid='+str(datasetid)+'&'+'units=standard'+'&'+'limit=1000'
    r = requests.get(base_url, headers = token, params=stations)
    print("Request status code: "+str(r.status_code))

    try:
        #results comes in json form. Convert to dataframe
        df = pd.DataFrame.from_dict(r.json()['results'])
        print("Successfully retrieved "+str(len(df['id'].unique()))+" stations")
        
        if df.count().max() >= 1000:
            print('reached limit = 1000')

 
        return df
    #Catch all exceptions for a bad request or missing data
    except:
        print("Error converting station data to dataframe. Missing data?")


