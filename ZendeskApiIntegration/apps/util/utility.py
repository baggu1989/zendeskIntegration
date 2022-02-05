import requests
import yaml
import pandas as pd
import numpy as np
import time

def invokeApi(url, data ,auth):
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, json=data, auth=auth, headers=headers)
    print(response.content)
    if response.status_code == 429:
        print('Rate limit reached. Please wait.')
        time.sleep(int(response.headers['retry-after']))
        response = requests.post(url, json=data, auth=auth, headers=headers)
    if response.status_code != 200:
        print(f'Import failed with status {response.status_code}')
        exit()
    return response.content,response.status_code


def getPropertyfromConfig(configFilePath):
    with open(configFilePath, 'r') as file:
        prime_service = yaml.safe_load(file)
        return prime_service

def getinputDatafromCsv(fileLocation):
    inputDataDf = pd.read_csv(fileLocation)
    return inputDataDf

def getSplittedDf(df):
    listUserDataDf = np.array_split(df, (len(df) / 100) + 1)
    return listUserDataDf

