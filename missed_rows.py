import pandas as pd
import main_columns
import sys
import colorama
import xlsxwriter

from loguru import logger
from datetime import time, datetime

class Missed_Rows:
    
    logger.remove()
    missedRowsLogger = logger.bind(name = 'missed_rows_logger').opt(colors = True)
    missedRowsLogger.add(sink = sys.stdout, format =  "<blue> {time:HH:mm:ss} </blue> | {message}", level = "INFO", colorize = True)
    
    def prepare_rows(self, docDf:pd.DataFrame, rdDf:pd.DataFrame) -> pd.DataFrame:

        Missed_Rows.missedRowsLogger.info("Finding information with missing data")
        cipherDf = pd.merge(docDf, rdDf,
                                how = 'outer',
                                on = 'Шифр',
                                suffixes=['', '_new'],
                                indicator = True)
        pathDf = cipherDf[cipherDf['_merge'] == 'right_only'][rdDf.columns]

        cipherCodeDf = pd.merge(docDf, pathDf,
                            how = 'outer',
                            left_on = 'Шифр',
                            right_on = 'Код',
                            suffixes=['', '_new'],
                            indicator=True)
        missedRows = cipherCodeDf[cipherCodeDf['_merge'] == 'right_only'][main_columns.missedColumns]
        missedRows.to_excel('123.xlsx', index = False)
        missedRows = missedRows.loc[missedRows['Система_new'].isin(list(set(docDf['Система'])))]
        missedRows = missedRows.dropna(subset = ['Система_new'])
        missedRows.columns = main_columns.missedColumnsNew

        Missed_Rows.missedRowsLogger.info('Missing value search finished')
        return missedRows




    

        