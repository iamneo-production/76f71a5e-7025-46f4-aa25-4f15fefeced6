import pandas as pd
import numpy as np
from scrape_data import COORDS
from scrape_data import create_datasets, create_test_dataset

def clean_dataset(df: pd.DataFrame):
    '''
    Convert date column to proper datetime format
    Interpolate any missing values
    '''
    df.date = pd.to_datetime(df.date, format=r"%Y%m%d")
    df['month'] = df.date.dt.month
    df.replace(-999.0,np.nan, inplace=True)


    for i in df.columns.drop('date'):
        df[i] = df[i].interpolate(method='linear')
    return df
    

'''
Standardization: Using the StandardScaler we will scale the following:
 - Temperature (T2M)
 - Wind Speed (WS2M)
 - Clearness Index (ALL_SKY_KT)
 - Soil Surface Moisture (GWETTOP)
 - Evapotranspiration Rate (EVLAND)
 - Surface Pressure (PS)
 '''
def scale_norm_dataset(df: pd.DataFrame):
    #Use standard scaling for the following
    from sklearn.preprocessing import StandardScaler
    standard_scaler = StandardScaler()
    standard_scaling_cols = ['T2M',
                            'WS2M',
                            'ALLSKY_KT',
                            'GWETTOP',
                            'EVLAND',
                            'PS']

    df[standard_scaling_cols] = standard_scaler.fit_transform(df[standard_scaling_cols])

    #Use MinMax Scaling for precipitation
    from sklearn.preprocessing import MinMaxScaler
    minmaxscaler = MinMaxScaler()
    df[['PRECIPITATIONCAL']] = minmaxscaler.fit_transform(df[['PRECIPITATIONCAL']])
    df.amplitude = df.amplitude/3
    df.RH2M = df.RH2M/100
    return df


'''
Cyclic Encoding:
- Apply Cyclic encoding to the WD2M parameter
- Drop WD2M
'''
def apply_cyclic_encoding(df: pd.DataFrame):
    df['WD2M_sin'] = np.sin(np.deg2rad(df.WD2M))
    df['WD2M_cos'] = np.cos(np.deg2rad(df.WD2M))
    df.drop(columns='WD2M', inplace=True)
    return df



def preprocess_data(city_name: str, verbose=False):
    if not city_name in COORDS.keys():
        raise Exception(f'Data not available for {city_name}. Please check the name and try again')
    
    if verbose:
        print('Original DF')
    try:
        print(f'data/district_data/{city_name}.csv')
        df = pd.read_csv(f'data/district_data/{city_name}.csv')
        if verbose:
            print(df)
    except FileNotFoundError:
        create_datasets()
        df = pd.read_csv(f'data/district_data/{city_name}.csv')
        if verbose:
            print(df)
    

    df = clean_dataset(df)
    df = scale_norm_dataset(df)
    df = apply_cyclic_encoding(df)
    if verbose:
        print('Processed DF')
        print(df)
    return df

    
if __name__ == '__main__':
    # print('karimnagar' in COORDS.keys())
    # create_datasets
    preprocess_data('karimnagar', verbose=True)