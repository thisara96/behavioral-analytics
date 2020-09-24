from firebase import firebase
import pandas as pd
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from flask import Flask, request, jsonify
import time


firebase = firebase.FirebaseApplication('https://esp-app-10.firebaseio.com/', None)

def last_date():
    result = firebase.get('/Sensor', '')
    for i in result:
        last_date = i
 
    return last_date

def last_val(last_date, ):
    result = firebase.get('/Sensor/{}'.format(last_date), '')
    return result[-1]


last_date_ = last_date()

result = firebase.get('/Sensor/09_21', '')

keys = result.keys()
keys = list(map(int, keys))

result = result[str(keys[-1])]

print(result)

print(last_val(last_date_))