from firebase import firebase
import pandas as pd
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from flask import Flask, request, jsonify
import time


firebase = firebase.FirebaseApplication('https://esp-app-10.firebaseio.com/', None)

list_R = ['Living Room', 'Room 02']

def write_db(count, list_data):
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    i = count % 2
    R = list_R[i]
    data = {
        'D' : dt_string,
        'R' : list_R[i]
    }
    print(list_data)
    list_data.append(data)
    result = firebase.put('/Sensor', '09_19', list_data)

def write_db_dict(count):
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    i = count % 2
    R = list_R[i]
    data = {
        'D' : dt_string,
        'R' : list_R[i]
    }
    print(data)
    result = firebase.put('/Sensor/09_23', str(count), data)

    #return list_data

count = 300
list_data = []
while(True):
    write_db_dict(count)
    count += 1
    time.sleep(60)
