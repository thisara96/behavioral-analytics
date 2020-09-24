from firebase import firebase
import pandas as pd
from datetime import datetime
import numpy as np
from fbprophet import Prophet
from datetime import date
from datetime import timedelta


firebase = firebase.FirebaseApplication('https://esp-app-10.firebaseio.com/', None)

def post_data(df, column,path):
    data = df.to_dict()
    result = firebase.put(path, column , data)

def prophet_preprocess(df, column):
    data = df.copy()
    data = data.rename(columns = {'D':'ds', column: 'y'})
    data['cap'] = 1
    data['floor'] = 0
    return data

def prophet_model(df):
    m = Prophet(changepoint_prior_scale=0.01)
    m.fit(df)
    future = m.make_future_dataframe(periods=1440, freq='min')
    future['cap'] = 1
    future['floor'] = 0
    forecast = m.predict(future)
    df_future = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    return df_future

def prophet_postprocessing(future , df):
    df_nextday = future.iloc[len(df) - 1:]
    print(len(df))
    print(len(df_nextday))
    print(len(future))

    df_nextday.set_index(['ds'], inplace = True)
    df_nextday = df_nextday.reset_index()

    df_nextday.loc[df_nextday['yhat'] >= 1, 'yhat'] = 1
    df_nextday.loc[df_nextday['yhat_upper'] >= 1, 'yhat_upper'] = 1
    df_nextday.loc[df_nextday['yhat_lower'] >= 1, 'yhat_lower'] = 1

    df_nextday.loc[df_nextday['yhat'] <= 0, 'yhat'] = 0
    df_nextday.loc[df_nextday['yhat_upper'] <= 0, 'yhat_upper'] = 0
    df_nextday.loc[df_nextday['yhat_lower'] <= 0, 'yhat_lower'] = 0

    df_nextday = df_nextday.rename(columns = {'ds':'time', 'yhat': 'mean', 'yhat_lower':'lower_bound', 'yhat_upper':'upper_bound'})

    df_nextday["minute"] = df_nextday['time'].map(lambda x: x.minute)
    df_nextday["hour"] = df_nextday['time'].map(lambda x: x.hour)
    df_nextday['minutes'] = df_nextday['minute'] + df_nextday['hour'] * 60
    df_nextday['time'] = datetime.combine(date.today() , datetime.min.time()) + pd.TimedeltaIndex(df_nextday['minutes'], unit='m')

    df_nextday.drop(['minutes', 'hour', 'minute'], axis=1, inplace=True)
    return df_nextday


def prophet_model_all_columns(df):
    data = df.copy()
    data = data[-5000:]
    firebase.delete('/', 'Model 01')
    rooms = data['R'].unique()
    list_temp = []
    for i in range(1):
        list_temp.append(rooms[i])
    dict_prophet = {}
    for column in rooms:
        df_ = prophet_preprocess(data, column)
        future = prophet_model(df_)


        future = prophet_postprocessing(future , df_)
        #result = firebase.get('/Model 01/{}'.format(column), '')
        #if result :

        path = 'Model 01/'
        post_data(future, column, path)
        future.set_index(['time'], inplace = True)

        dict_prophet[column] = future.copy()

    return dict_prophet

def std(x): 
    return np.std(x)

def mean_std_model(data):
    df = data.copy()
    df["minute"] = df['D'].map(lambda x: x.minute)
    df["hour"] = df['D'].map(lambda x: x.hour)
    df_g = df.groupby(['hour','minute']).agg(['mean', std])
    df = df_g.reset_index()
    df['minutes'] = df['minute'] + df['hour'] * 60
    df['time'] = datetime.combine(date.today() , datetime.min.time()) + pd.TimedeltaIndex(df['minutes'], unit='m')
    df.drop(['minutes', 'hour', 'minute'], axis=1, inplace=True)

    return df

def get_bounds(data, column):
    df = data.copy()
    df = df.set_index(['time'])
    mean_series = df.loc[:, (column, 'mean')].copy()
    std_series = df.loc[:, (column, 'std')].copy()

    mean_df = pd.DataFrame({'mean': mean_series, 'std': std_series})

    upper_bound = []
    lower_bound = []
    num_rows = mean_df.shape[0]
    for i in range(num_rows):
        upper_bound.append(mean_df.iloc[i]['mean'] + 2 * mean_df.iloc[i]['std'])
        lower_bound.append(mean_df.iloc[i]['mean'] - 2 * mean_df.iloc[i]['std'])
    
    mean_df['upper_bound'] = upper_bound
    mean_df['lower_bound'] = lower_bound

    mean_df = mean_df.reset_index()
    mean_df.drop('std', axis=1, inplace=True)
    return mean_df

def mean_std_postprocessing(df):
    df_nextday = df.copy()
    df_nextday.loc[df_nextday['mean'] >= 1, 'mean'] = 1
    df_nextday.loc[df_nextday['upper_bound'] >= 1, 'upper_bound'] = 1
    df_nextday.loc[df_nextday['lower_bound'] >= 1, 'lower_bound'] = 1

    df_nextday.loc[df_nextday['mean'] <= 0, 'mean'] = 0
    df_nextday.loc[df_nextday['upper_bound'] <= 0, 'upper_bound'] = 0
    df_nextday.loc[df_nextday['lower_bound'] <= 0, 'lower_bound'] = 0

    return df_nextday 

def mean_std_all_columns(df):
    data = df.copy()
    rooms = data['R'].unique()
    firebase.delete('/', 'Model 02')
    data = mean_std_model(data)

    dict_mean_std = {}
    for column in rooms:
        df_ = get_bounds(data, column)
        df_ = mean_std_postprocessing(df_)
        path = 'Model 02/'


        post_data(df_, column, path)

        df_.set_index(['time'], inplace = True)
        dict_mean_std[column] = df_.copy()
    
    return dict_mean_std
