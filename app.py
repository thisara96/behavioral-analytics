from firebase import firebase
import pandas as pd
from datetime import datetime
import numpy as np
from fbprophet import Prophet
from datetime import date
import time
from flask import request, Flask
from flask import render_template

import atexit
from apscheduler.schedulers.background import BackgroundScheduler

from models.markov import markov_model, store_markov_model, transition_matrix
from models.temporal import prophet_model_all_columns, mean_std_all_columns

app = Flask(__name__)

firebase = firebase.FirebaseApplication('https://esp-app-10.firebaseio.com/', None)


def onehot_encode(df):
    data = df.copy()
    rooms = data['R'].unique()
    for each in rooms:
        data[f'{each}'] = data['R'].apply(lambda x: 1 if x == each else 0)
    #data.drop('R', axis=1, inplace=True)
    return data



def get_dataframe():
    result = firebase.get('/Sensor', '')
    list_dict = []

    for i in result:
        last_date = i
    for i in result:
        store = i
        if i == last_date :
            break
        for j in result[i]:
            list_dict.append(j)
    
    data = pd.DataFrame(list_dict)
    

    data['D'] = pd.to_datetime(data['D'])

    df = data.copy()
    df['D'] = data['D'].dt.strftime('%d-%m-%Y %H:%M')
    df['D'] = pd.to_datetime(df['D'])

    return df

def last_date():
    result = firebase.get('/Sensor', '')
    for i in result:
        last_date = i

    return last_date

def last_val(last_date):
    result = firebase.get('/Sensor/{}'.format(last_date), '')
    print(result)
    return result[-1]


def last_val_df(last_date, dict_format = False):
    if dict_format :
        last_value = last_val_dict_format(last_date)
    else : 
        last_value = last_val(last_date)
    print(last_value)
    list_val_list = [last_value]
    print(list_val_list)
    data = pd.DataFrame(list_val_list)
    print(data)

    data['D'] = pd.to_datetime(data['D'])

    df = data.copy()

    df['D'] = data['D'].dt.strftime('%d-%m-%Y %H:%M')
    df['D'] = pd.to_datetime(df['D'])

    #df.set_index(['D'], inplace=True)
    return df

def last_val_dict_format(last_date):

    result = firebase.get('/Sensor/{}'.format(last_date), '')
    print(result)
    keys = result.keys()
    keys = list(map(int, keys))

    result = result[str(keys[-1])]
    return result



def add_outlier():
    result = firebase.put('/', 'Outliers', {'Transition Outlier': False, 'Temporal Outlier': False})
    #result = firebase.put('/', 'Outliers', {'Temporal Outlier': False})


#data = get_dataframe()
print(".....")
prev_value = None
data = None
df = None
dict_prophet = None
dict_mean_std = None
matrix = None
count_transition = 0
count_temporal = 0
df_set = 0


@app.route('/')
def index():
    global dict_prophet
    global prev_value
    return render_template('index.html', df=dict_prophet, time=prev_value)


def outlier_detection():
    global prev_value
    global data
    global df
    global dict_prophet
    global dict_mean_std
    global matrix
    global count_temporal
    global count_transition
    global df_set



    last_date_ = last_date()
    #print(last_date_)
    last_val_ = last_val_df(last_date_, dict_format=True)
    print(dict_prophet)
    print(".....")

    time_ = last_val_['D'][0]
    print(time_)
    if (time_.hour == 0 and time_.minute == 0) or (df_set == 0):
        print('new df is creating')
        data = get_dataframe()
        df = onehot_encode(data)

        dict_prophet = prophet_model_all_columns(df)
        dict_mean_std = mean_std_all_columns(df)

        matrix = transition_matrix(data)

        result = firebase.put('/Outliers', 'Temporal Outlier', False)
        result = firebase.put('/Outliers', 'Transition Outlier', False)
        result = firebase.delete('/Outliers', 'Transition Outlier Time')
        result = firebase.delete('/Outliers', 'Temporal Outlier Time')
    
        count_transition = 0
        count_temporal = 0
        df_set = 1
    #print(dict_mean_std)
    try : 
        df_detect = dict_prophet[last_val_['R'][0]].copy()
        print('try ... detect')
    except :
        print("Outlier detected")
        result = firebase.put('/Outliers', 'Temporal Outlier', True)
        result = firebase.put('/Outliers', 'Transition Outlier', True)
        data_trans = {
                'time': last_val_['D'][0],
                'room_prev': prev_value,
                'room_current': last_val_['R'][0]
            }
        result = firebase.put('/Outliers/Transition Outlier Time', str(count_transition), data_trans)

        data_temp = {
            'time': last_val_['D'][0],
            'room': last_val_['R'][0]
        }
        result = firebase.put('/Outliers/Temporal Outlier Time', str(count_temporal), data_temp)
        count_transition += 1
        count_temporal += 1
        return

    try :
        df_detect = dict_prophet[last_val_['R'][0]].copy()
        val = df_detect.loc[str(last_val_['D'][0])]
        val = val.reset_index()
        val = val['upper_bound'][0]
        print('try ... val')
    except :
        print("Today's data is not added")
        return
        
    print('return ... ')
    print(val)
    if val < 1 :
        print('prophet outlier detected')
        df_mean_defect = dict_mean_std[last_val_['R'][0]].copy()
        val_mean = df_mean_defect.loc[str(last_val_['D'][0])]
        if val_mean['upper_bound'] < 1 :
            print('Outlier detected..... ')
            result = firebase.put('/Outliers', 'Temporal Outlier', True)
            data_temp = {
                'time': last_val_['D'][0],
                'room': last_val_['R'][0]
            }
            result = firebase.put('/Outliers/Temporal Outlier Time', str(count_temporal), data_temp)
            count_temporal += 1
        else :
            print('No Outlier detected')
    else :
        print('No Outlier detected')

    current = last_val_['R'][0]
    if prev_value != None :
        if matrix[prev_value][current] < 0.001 :
            print('Transition Outlier detected')
            result = firebase.put('/Outliers', 'Transition Outlier', True)
            data_trans = {
                'time': last_val_['D'][0],
                'room_prev': prev_value,
                'room_current': current
            }
            result = firebase.put('/Outliers/Transition Outlier Time', str(count_transition), data_trans)
            count_transition += 1
        else:
            print("No transition outliers")

    prev_value = current



scheduler = BackgroundScheduler()
scheduler.add_job(outlier_detection,'interval',minutes=1)
scheduler.start()
# Shutdown your cron thread if the web process is stopped
atexit.register(lambda: scheduler.shutdown(wait=False))


if __name__ == '__main__':
	app.run()