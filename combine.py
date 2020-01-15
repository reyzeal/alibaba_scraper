from __future__ import unicode_literals

import os
import shutil
import pandas as pd
import xlsxwriter

###########################################################
DIRECTORY = 'scrap_files'
FILENAME = 'gzhengdian'
###########################################################

def combine(DIRECTORY, FILENAME):
    DIRNAME = os.path.join(DIRECTORY, 'temp')

    first = False
    cols = []
    workbook = xlsxwriter.Workbook(f'{DIRECTORY}/{FILENAME}.xlsx')
    worksheet = workbook.add_worksheet()
    row = 0
    for i in os.scandir(DIRNAME):
        if FILENAME in str(i.name) and 'json' not in str(i.name):
            try:
                print(i.name)
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
            except IndexError:
                print("Corrupted file:"+i.name)
    workbook.close()
    shutil.rmtree(DIRNAME, ignore_errors=True)


if __name__ == '__main__':
    combine(DIRECTORY, FILENAME)
