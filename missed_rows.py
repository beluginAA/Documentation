import pandas as pd
import main_columns
import sys
import xlsxwriter
import colorama

from loguru import logger
from datetime import time, datetime

class Missed_Rows:
    
    logger.remove()
    missed_rows_logger = logger.bind(name = 'missed_rows_logger').opt(colors = True)
    missed_rows_logger.add(sink = sys.stdout, format =  "<blue> {time:HH:mm:ss} </blue> | {message}", level = "INFO", colorize = True)

    def __init__(self, df_1, df_2):
        self.df_1 = df_1
        self.df_2 = df_2

    @staticmethod
    def list_matching(df, array):
        if df['Система_new'] in list(array):
            return df
        else:
            return None
    
    def prepare_rows(self, df_1, df_2):

        Missed_Rows.missed_rows_logger.info("Finding information with missing data")

        merged1_df = pd.merge(df_1, df_2,
                                how = 'right',
                                on = 'Шифр',
                                suffixes=['', '_new'],
                                indicator = True)
        right_only = merged1_df[merged1_df['_merge'] == 'right_only'][df_1.columns]
        merged2_df = pd.merge(right_only, df_2,
                            how = 'right',
                            left_on = 'Шифр',
                            right_on = 'Код',
                            suffixes=['', '_new'],
                            indicator=True)
        missed_rows = merged2_df[merged2_df['_merge'] == 'right_only'][list(main_columns.missed_columns)]

        system_array = df_1['Система'].drop_duplicates()
        self.summary_missed_rows = missed_rows.apply(lambda row: self.list_matching(row, system_array), axis = 1)
        self.summary_missed_rows = self.summary_missed_rows.dropna(subset = ['Система_new'])
        self.summary_missed_rows.columns = main_columns.missed_rows_new
    
    def upload_rows(self):

        styler = self.summary_missed_rows.style
        styler.set_properties(**{'border': '1px solid black', 'border-collapse': 'collapse'})
        writer = pd.ExcelWriter('./Missed_rows.xlsx', engine='xlsxwriter')
        styler.to_excel(writer, sheet_name='Пропущенные значения', encoding='cp1251', index=False)

        workbook = writer.book
        worksheet = writer.sheets['Пропущенные значения']
        worksheet.autofilter(0, 0, len(self.summary_missed_rows.index), len(self.summary_missed_rows.columns) - 1)

        for i, column in enumerate(self.summary_missed_rows.columns):
            column_width = max(self.summary_missed_rows[column].astype(str).map(len).max(), len(column))
            worksheet.set_column(i, i, column_width)

        writer.save()

        Missed_Rows.missed_rows_logger.info('Missing value search finished')
    

        