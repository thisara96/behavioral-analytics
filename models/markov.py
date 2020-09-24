from firebase import firebase
import pandas as pd
from datetime import datetime
import numpy as np
from fbprophet import Prophet
from datetime import date

firebase = firebase.FirebaseApplication('https://esp-app-10.firebaseio.com/', None)

def markov_model(data):
    Transition_Matrix = {}
    count_dict = {}

    rooms = data['R'].unique()
    for i in rooms :
        Transition_Matrix[i] = {}
        count_dict[i] = 0
        for j in rooms :
            Transition_Matrix[i][j] = 0

    prev = data['R'].loc[0]

    for i in range(1, len(data)):
        current = data['R'].loc[i]
        Transition_Matrix[prev][current] += 1
        count_dict[prev] += 1
        prev = current

    for i in Transition_Matrix :
        for j in Transition_Matrix[i] :
            Transition_Matrix[i][j] = Transition_Matrix[i][j] / count_dict[i]
            Transition_Matrix[i][j] = round(Transition_Matrix[i][j], 4)

    return Transition_Matrix

def store_markov_model(matrix):
    result = firebase.put('/', 'Model 03', matrix)


def transition_matrix(data):
    matrix = markov_model(data)
    store_markov_model(matrix)

    return matrix