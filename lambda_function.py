import pandas as pd
import os

def lambda_handler(event, context):
    d = {'col1': [1,2], 'col2': [3,4]}
    df = pd.DataFrame(data = d)
    print(df)
    print('Done x2.1')
    current_working_directory = os.getcwd()
    print('Current working directory', current_working_directory)
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    for f in files:
        print(f)