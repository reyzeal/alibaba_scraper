import xlsxwriter
import os
import pandas as pd
###########################################################
DIRECTORY = 'alibaba'
FILENAME = 'alibaba_jewelry'
###########################################################
DIRNAME = os.path.join(os.path.dirname(__file__), DIRECTORY)

first = False
dataframes = []
cols = []
for i in os.scandir(DIRNAME):
    if FILENAME in str(i):
        if first is False:
            wb = pd.read_excel(os.path.join(DIRNAME,'temp',i))
            cols = wb.columns.tolist()
            print(cols)
            dataframes.append(wb)
            first = True
        else:
            wb = pd.read_excel(os.path.join(DIRNAME,'temp', i), skiprows=1)
            dataframes.append(wb)

dataframes = pd.concat(dataframes, ignore_index=True, sort=False)
dataframes = dataframes[cols]
dataframes = dataframes[dataframes.filter(regex='^(?!Unnamed)').columns]

dataframes.to_excel(os.path.join(DIRNAME,FILENAME+'.xlsx'))
