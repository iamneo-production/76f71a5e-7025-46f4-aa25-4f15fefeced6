
import sys
sys.path.append('data')
import preprocessing
import scrape_data
from darts import TimeSeries
import pandas as pd
from typing import Union

def create_timeseries(train, test):
    series = TimeSeries.from_dataframe(train, "date", train.columns.drop('date'))
    target = series['T2M']
    train_series = target
    test_series = TimeSeries.from_dataframe(test, "date", ["T2M",'month'])

    past_covariates = series[['RH2M',
                            'WS2M',
                            'ALLSKY_KT',
                            'GWETTOP',
                            'EVLAND',
                            'PS',
                            'PRECIPITATIONCAL',
                            'MEI',
                            'PC1',
                            'PC2',
                            'amplitude',
                            'month',
                            'WD2M_sin',
                            'WD2M_cos']]
    future_covariates = series['month'].append(test_series['month'])
    return test_series, train_series, past_covariates, future_covariates





from darts.models.forecasting.tcn_model import TCNModel
from darts.models.forecasting.tft_model import TFTModel
import torch
def train_tcn(train_series, past_covariates):
    Heatwave_TCNModel = TCNModel(
                        model_name='TCN_Heatwave',
                        loss_fn=torch.nn.HuberLoss(),
                        input_chunk_length=1095,
                        output_chunk_length=365,
                        batch_size=256,
                        n_epochs=1000,
                        add_encoders={
                            'cyclic': {'past':['month']},
                            },
                        force_reset=True,
                        pl_trainer_kwargs={
                            "accelerator": "gpu",
                            "devices": [0]
                        },
                        log_tensorboard=True
                        )
    Heatwave_TCNModel.fit(
                    train_series, 
                    verbose=True,
                    past_covariates = past_covariates)
    Heatwave_TCNModel.save('\darts_logs\TCN_Heatwave\TCN_Heatwave.pt')


def train_tft(train_series, past_covariates, future_covariates):
    Heatwave_TFTModel = TFTModel(
                     model_name='Heatwave_TFT',
                     input_chunk_length=365,
                     output_chunk_length=365,
                     batch_size=32,
                     # torch_metrics=regression_metrics,
                     full_attention=True,
                     feed_forward='ReGLU',
                     n_epochs=50,
                     add_encoders={
                        'cyclic': {'future': ['month'], 'past':['month']},
                        },
                     force_reset=True,
                     pl_trainer_kwargs={
                        "accelerator": "gpu",
                        "devices": [0]
                      },
                      log_tensorboard=True
                    )
    Heatwave_TFTModel.fit(
                    train_series, 
                    verbose=True,
                    past_covariates = past_covariates,
                    future_covariates = future_covariates)
    Heatwave_TFTModel.save('\darts_logs\Heatwave_TFT\Heatwave_TFT.pt')
    pass

'''
Model must be either 'tcn' or 'tft'
'''
def train_model(model: str, city:str):
    train_df = preprocessing.preprocess_data(city, verbose=True)
    test_df = scrape_data.create_test_dataset(city, '20220101', '20221231')
    test_series, train_series, past_covariates, future_covariates = create_timeseries(test=test_df, train=train_df)
    if model == 'tft':
        train_tft(train_series=train_series,
                  past_covariates=past_covariates,
                  future_covariates=future_covariates)
        
    if model == 'tcn':
        train_tcn(train_series=train_series,
                  past_covariates=past_covariates)

def predict(model: str, city:str, verbose=False):
    train_df = preprocessing.preprocess_data(city, verbose=True)
    test_df = scrape_data.create_test_dataset(city, '20220101', '20221231')
    test_series, train_series, past_covariates, future_covariates = create_timeseries(test=test_df, train=train_df)
    if model == 'tft':
        Heatwave_TFTModel = TFTModel.load(r'models\darts_logs\Heatwave_TFT\Heatwave_TFT.pt')
        prediction = Heatwave_TFTModel.predict(n=365, 
                                               past_covariates=past_covariates, 
                                               future_covariates=future_covariates)
        if verbose:
            print(prediction)
        return prediction
        
    if model == 'tcn':
        Heatwave_TCNModel = TCNModel.load(r'models\darts_logs\TCN_Heatwave\TCN_Heatwave.pt')
        prediction = Heatwave_TCNModel.predict(n=365, 
                                               past_covariates=past_covariates)
        if verbose:
            print(prediction)
        return prediction
    

if __name__ == '__main__':
    # TFTModel.load('models/darts_logs/Heatwave_TFT/Heatwave_TFT.pt')
    predict(model='tft', city='karimnagar')