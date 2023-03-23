import pandas as pd
import main_columns
import sys

from loguru import logger
from datetime import datetime


class Log_File:

    log_file_logger = logger.bind(name = 'log_file_logger').opt(colors = True)
    log_file_logger.add(sink = sys.stdout, format =  "<red> {time:HH:mm:ss} </red> | {message}", level = "INFO")

    def __init__(self, df_1, df_2):
        self.df_1 = df_1
        self.df_2 = df_2
    
    @staticmethod
    def change_columns(row, column):
        if column[0] != f'{column[1]}_new':
            if row[column[0]] == row[column[1]]:
                return '-'
            else:
                return f'Смена {column[0].lower()} c {row[column[0]]} на {row[column[1]]}'
        else:
            if row[column[0]] == row[column[1]]:
                return '-'
            else:
                return f'Смена {column[0].lower()} c {row[column[0]]} на {row[column[1]]}' 
            
    @staticmethod
    def change_code_new(row, column):
        if row['Шифр_new'] != '':
            return f'Смена шифра c {row[column[0]]} на {row[column[1]]}'
        else:
            return '-'
    
    @staticmethod
    def change_status_new(df):
        if pd.isna(df['Итог_статус']):
            return 'Отсутствует'
        elif 'ВК+' in df['Итог_статус'] or 'Выдан в производство' in df['Итог_статус']:
            return 'Утвержден'
        else:
            return 'На согласовании'

    @staticmethod
    def change_type_new(df):
        if pd.isna(df['Тип']) and ~pd.isna(df['Тип_new']):
            return f'Смена типа c {df["Тип"]} на {df["Тип_new"]}'
        else:
            return df['Тип'] 

    def prepare_data(self, df_1, df_2):
        
        Log_File.log_file_logger.info('Preparing data for log-file')
        
        df_1['Тип'] = df_1.apply(self.change_type_new, axis = 1)
        df_2['Тип'] = df_2.apply(self.change_type_new, axis = 1)
        df_1['Итог_статус'] = df_1.apply(self.change_status_new, axis = 1)
        df_2['Итог_статус'] = df_2.apply(self.change_status_new, axis = 1)

        for column in main_columns.changed_columns:
            if 'Код' not in column:
                df_1[column[0]] = df_1.apply(lambda row: self.change_columns(row, column), axis=1)
                df_2[column[0]] = df_2.apply(lambda row: self.change_columns(row, column), axis=1)
            else:
                df_1['Шифр'] = '-'
                df_2['Шифр'] = df_2.apply(lambda row: self.change_code_new(row, column), axis = 1)
    
    def upload_file(self, df_1, df_2):

        Log_File.log_file_logger.info('Uploading modified data to a document')

        log_df = pd.concat([df_1[list(main_columns.log_file_columns)], df_2[list(main_columns.log_file_columns)]])
        log_df = log_df.reset_index()[list(main_columns.log_file_columns)]
        max_len_row = [max(log_df[row].apply(lambda x: len(str(x)) if x else 0)) for row in log_df.columns]
        max_len_name = [len(row) for row in main_columns.log_file_columns]
        max_len = [col_len if col_len > row_len else row_len for col_len, row_len in zip(max_len_name, max_len_row)]
        output_filename = 'log-RD-' + datetime.now().isoformat(timespec='minutes').replace(':', '_')
        with open(f'{output_filename}.txt', 'w') as log_file:
            log_file.write('Список измененных значений:\n')
            log_file.write('\n')
            file_write = ' ' * (len(str(log_df.index.max())) + 3)
            for column, col_len in zip(log_df.columns, max_len):
                file_write += f"{column:<{col_len}}|"
            log_file.write(file_write)
            log_file.write('\n')
            for index, row in log_df.iterrows():
                column_value = ''
                for i in range(len(log_df.columns)):
                    column_value += f"{str(row[log_df.columns[i]]) if row[log_df.columns[i]] else '-':<{max_len[i]}}|"
                log_file.write(f"{index: <{len(str(log_df.index.max()))}} | {column_value}\n")
            log_file.write('\n')
        
        Log_File.log_file_logger.info('Log-file ready')

