import pandas as pd
import main_columns
import sys

from loguru import logger
from datetime import datetime


class Log_File:

    logFileLogger = logger.bind(name = 'log_file_logger').opt(colors = True)
    logFileLogger.add(sink = sys.stdout, format =  "<red> {time:HH:mm:ss} </red> | {message}", level = "DEBUG")

    def __init__(self, cipherDf:pd.DataFrame, cipherCodeDf:pd.DataFrame):
        self.cipherDf1 = cipherDf
        self.cipherCodeDf = cipherCodeDf
    
    @staticmethod
    def change_columns(df:pd.DataFrame, column:str) -> str:
        if column[0] != f'{column[1]}_new':
            if df[column[0]] == df[column[1]]:
                return '-'
            else:
                if df[column[0]] is None and df[column[1]] in [None, '']:
                    return '-'
                elif df[column[0]] == None:
                    return f'Смена {column[0].lower()} c на {df[column[1]]}'
                else:
                    return f'Смена {column[0].lower()} c {df[column[0]]} на {df[column[1]]}'
        else:
            if df[column[0]] == df[column[1]]:
                return '-'
            else:
                return f'Смена {column[0].lower()} c {df[column[0]]} на {df[column[1]]}' 
            
    @staticmethod
    def change_code_new(df:pd.DataFrame, column:str) -> str:
        if df['Шифр_new'] != '':
            return f'Смена шифра c {df[column[0]]} на {df[column[1]]}'
        else:
            return '-'
    
    @staticmethod
    def change_status_new(df:pd.DataFrame) -> str:
        if pd.isna(df['Итог_статус']):
            return 'Отсутствует'
        elif 'ВК+' in df['Итог_статус'] or 'Выдан в производство' in df['Итог_статус']:
            return 'Утвержден'
        else:
            return 'На согласовании'

    @staticmethod
    def change_type_new(df:pd.DataFrame) -> str:
        if pd.isna(df['Тип'])  and ~pd.isna(df['Тип_new']):
            return f'Смена типа c на {df["Тип_new"]}'
        else:
            return df['Тип'] 

    def prepare_data(self, cipherDf:pd.DataFrame, cipherCodeDf:pd.DataFrame) -> pd.DataFrame:
        
        Log_File.logFileLogger.info('Preparing data for log-file')

        cipherDf['Тип'] = cipherDf.apply(self.change_type_new, axis = 1)
        cipherCodeDf['Тип'] = cipherCodeDf.apply(self.change_type_new, axis = 1)
        cipherDf['Итог_статус'] = cipherDf.apply(self.change_status_new, axis = 1)
        cipherCodeDf['Итог_статус'] = cipherCodeDf.apply(self.change_status_new, axis = 1)

        for column in main_columns.changedColumns:
            if 'Код' not in column:
                cipherDf[column[0]] = cipherDf.apply(lambda row: self.change_columns(row, column), axis=1)
                cipherCodeDf[column[0]] = cipherCodeDf.apply(lambda row: self.change_columns(row, column), axis=1)
            else:
                cipherDf['Шифр'] = '-'
                cipherCodeDf['Шифр'] = cipherCodeDf.apply(lambda row: self.change_code_new(row, column), axis = 1)
        logDf = pd.concat([cipherDf[list(main_columns.logFileColumns)], cipherCodeDf[list(main_columns.logFileColumns)]])
        logDf = logDf.reset_index()[list(main_columns.logFileColumns)]

        Log_File.logFileLogger.info('Log-file ready')
        return logDf
    
