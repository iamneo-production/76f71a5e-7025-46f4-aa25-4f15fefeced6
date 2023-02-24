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
def get_nasa_data(coords: tuple, filename: str):
    url =  "https://power.larc.nasa.gov/api/temporal/daily/point"

    params = {
        "parameters":'T2M,RH2M,WS2M,WD2M,ALLSKY_KT,GWETTOP,EVLAND',
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
    df.to_csv(f'data/{filename}.csv')

    return df

def get_climate_data():
    coords = {'karimnagar': (18.44, 79.13),
              'warangal':(18.00,79.59),
              'adilabad':(19.67,78.53),
              'nizamabad':(18.67, 78.10),
              'khammam':(17.25,80.15)}
    
    for i in coords:
        get_nasa_data(coords=coords[i], filename=f'district_data/{i}')



if __name__ == '__main__':
    get_climate_data()
