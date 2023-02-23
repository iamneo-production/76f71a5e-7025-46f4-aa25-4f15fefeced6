import requests
import pandas as pd
import json

'''
We are going to retrieve the following from https://power.larc.nasa.gov/
- Avg Temp at 2m
- RH at 2m
- Wind Speed at 2m
- Wind Direction at 2m
- Midday Insolation
- Surface Soil Moisture
- Cloud Cover
'''
def get_nasa_data(coords: tuple, filename: str):
    url =  "https://power.larc.nasa.gov/api/temporal/daily/point"

    params = {
        "parameters":'T2M,RH2M,WS2M,WD2M,ALLSKY_SFC_SW_DWN',
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
    KARIMNAGAR_COORDS = (18.44, 79.13)
    get_nasa_data(KARIMNAGAR_COORDS, filename='karimnagar/karimnagar_nasa')
if __name__ == '__main__':
    get_climate_data()
