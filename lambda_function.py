import pandas as pd
import os
import eod_usrate as UR

def listDir(dir, level=50):
    if level > 0:
        level = level-1
        print('List directory: ', dir)
        files = [f for f in os.listdir(dir) if os.path.isfile(f)]
        print('Files: ', files)
        namelist = [ f.name for f in os.scandir(dir) if f.is_dir() ]
        print('Sub-Directories: ', namelist)
        subfolders = [ f.path for f in os.scandir(dir) if f.is_dir() ]
        print('Sub-Path: ', subfolders)
        for folder in subfolders:
            listDir(folder, level)

def env_display():
    d = {'col1': [1,2], 'col2': [3,4]}
    df = pd.DataFrame(data = d)
    current_working_directory = os.getcwd()
    listDir(current_working_directory)
    listDir('/opt', level=4)
    print(df)
    print('Done x2.2')

def lambda_handler(event, context):
    env_display()
    UR.eod_usrate_main()