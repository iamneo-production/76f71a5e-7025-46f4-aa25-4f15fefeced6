import requests
import pandas as pd
import json

'''
We are going to retrieve the following from https://power.larc.nasa.gov/
- Avg Temp at 2m
- RH at 2m
- Wind Speed at 2m
- Wind Direction at 2m
- Allsky Clearness Index
- Surface Soil Moisture
- Evapotranspiration rate
'''

COORDS = {'karimnagar': (18.44, 79.13),
            'warangal':(18.00,79.59),
            'adilabad':(19.67,78.53),
            'nizamabad':(18.67, 78.10),
            'khammam':(17.25,80.15)}
def get_nasa_data(coords: tuple, filename: str):
    url =  "https://power.larc.nasa.gov/api/temporal/daily/point"

    params = {
        "parameters":'T2M,RH2M,WS2M,WD2M,ALLSKY_KT,GWETTOP,EVLAND,PS',
        "start": "20150101",
        "end": "20211231",
        "community": "RE",
        "latitude": str(coords[0]),
        "longitude": str(coords[1]),
        "user":"DAVEDownload",
        "format":"JSON",
        
    }

    response = requests.get(url, params=params)
    json_data = json.loads(response.text)
    json_data['properties']['parameter']
    df = pd.DataFrame(json_data['properties']['parameter'])


    params['parameters'] = 'PRECIPITATIONCAL'
    params['time-standard'] = 'utc'
    response = requests.get(url,params=params)
    json_data = json.loads(response.text)
    json_data['properties']['parameter']
    df2 = pd.DataFrame(json_data['properties']['parameter'])

    df = pd.concat([df,df2],axis=1)
    df.to_csv(f'data/{filename}.csv', index_label='date')

    return df

def parse_MJO_data(filepath:str):
    with open(file=filepath) as data_file:
        data = {}
        for index, line in enumerate(data_file.readlines()[1:]):
            line = line.split()
            date = str(''.join([line[0], f'{int(line[1]):02d}', f'{int(line[2]):02d}']))
            data[index] = [int(date),*line[4:]]
        # return pd.DataFrame(data)
        # print(data)
        df = pd.DataFrame().from_dict(data=data, orient='Index', columns=['date','PC1','PC2','amplitude']) 
        return df
    
def parse_MEI_data(filepath:str):
    with open(file=filepath) as data_file:
        data = {}
        for line in data_file.readlines()[1:]:
            line = line.split()
            data[line[0]] = line[1:]
        return pd.DataFrame().from_dict(data, orient='Index')
    
def get_climate_data():
    for i in COORDS:
        get_nasa_data(coords=COORDS[i], filename=f'district_data/{i}_nasa_data')

import os
def create_datasets():
    get_climate_data()
    for i in COORDS:
        df = pd.read_csv(f'data\district_data\{i}_nasa_data.csv')
        MEI_data = parse_MEI_data('data\world_data\MEI.txt')
        MJO_data = parse_MJO_data('data\world_data\MJO_data.txt')
        
        df['MEI'] = df.date.apply(lambda x: MEI_data.loc[str(x)[:4]][int(str(x)[4:6]) - 1]) # Find the correspoding value based on the date and month. 
        df = df.merge(MJO_data, on='date', how='left')
        df.to_csv(f'data\district_data\{i}.csv', index=False)
        os.remove(f'data\district_data\{i}_nasa_data.csv')

import numpy as np
def create_test_dataset(cityname: str,from_date: str, to_date:str):
    url =  "https://power.larc.nasa.gov/api/temporal/daily/point"

    params = {
        "parameters":'T2M',
        "start": from_date, #Get data from 2015 because we use standard scaler to scale, and test data is from 2015 to 2021
        "end": to_date,
        "community": "RE",
        "latitude": COORDS[cityname][0],
        "longitude": COORDS[cityname][1],
        "user":"DAVEDownload",
        "format":"JSON",
        
    }

    response = requests.get(url, params=params)
    json_data = json.loads(response.text)
    json_data['properties']['parameter']
    test_df = pd.DataFrame(json_data['properties']['parameter'])
    # test_df.replace(-999.0, np.nan, inplace=True)
    test_df['date'] = test_df.index
    test_df['date'] = pd.to_datetime(test_df.date, format=r"%Y%m%d")
    test_df.replace(-999.0,np.nan, inplace=True)
    test_df['T2M'] = test_df['T2M'].interpolate(method='linear')
    test_df.T2M.astype(np.float64)

    test_df['month'] = test_df.date.dt.month
    test_df
    return test_df
        
if __name__ == '__main__':
    create_datasets()
