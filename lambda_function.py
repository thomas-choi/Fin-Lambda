import pandas as pd
import os

def listDir(dir):
    print('List directory: ', dir)
    files = [f for f in os.listdir(dir) if os.path.isfile(f)]
    for f in files:
        print(f)  
    namelist = [ f.name for f in os.scandir(dir) if f.is_dir() ]
    print('Sub-Directories: ', namelist)
    subfolders = [ f.path for f in os.scandir(dir) if f.is_dir() ]
    print('Sub-Path: ', subfolders)
    for folder in subfolders:
        listDir(folder)

def env_display():
    d = {'col1': [1,2], 'col2': [3,4]}
    df = pd.DataFrame(data = d)
    print(df)
    print('Done x2.1')
    current_working_directory = os.getcwd()
    listDir(current_working_directory)
    listDir('/opt')


def lambda_handler(event, context):
    env_display()