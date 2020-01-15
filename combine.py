import os

import numpy as np
import pandas as pd

###########################################################
import xlsxwriter

DIRECTORY = 'alibaba'
FILENAME = 'alibaba_jewelry'


###########################################################

def combine(DIRECTORY, FILENAME):
    DIRNAME = os.path.join(DIRECTORY, 'temp')

    first = False
    dataframes = pd.DataFrame()
    cols = []
    workbook = xlsxwriter.Workbook(f'{DIRECTORY}/{FILENAME}.xlsx')
    worksheet = workbook.add_worksheet()
    row = 0
    for i in os.scandir(DIRNAME):
        if FILENAME in str(i.name) and 'json' not in str(i.name):
            wb = pd.read_excel(os.path.join(DIRNAME, i.name)).fillna('')
            if first is False:
                cols = wb.columns.tolist()
                for j, k in enumerate(cols):
                    worksheet.write(row, j, k)
                row += 1
                first = True
            for j, k in wb.iterrows():
                for col, val in enumerate(cols):
                    if k[cols[0]] == '':
                        break
                    else:
                        worksheet.write(row, col, k[val])
                if k[cols[0]] != '':
                    row += 1

    # dataframes = pd.concat(dataframes, ignore_index=False, sort=False)
    # dataframes = dataframes[cols]
    # dataframes = dataframes[dataframes.filter(regex='^(?!Unnamed)').columns]
    # dataframes.to_excel(os.path.join(DIRECTORY, FILENAME + '.xlsx'))
    # print(dataframes.shape)
    workbook.close()


if __name__ == '__main__':
    combine(DIRECTORY, FILENAME)
