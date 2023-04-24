import pandas as pd


class Functions:

    def change_code(self, df:pd.DataFrame) -> str:
        if df['Шифр_new'] != '':
            return df['Шифр_new']
        else:
            return df['Код']
    
    def finding_empty_rows(self, df:pd.DataFrame, column:str) -> str:
        if df[column] in ['nan', 'None', '0'] or pd.isna(df[column]):
            return None
        else:
            return df[column]

    def change_status(self, df:pd.DataFrame) -> str:
        if pd.isna(df['Статус']):
            return 'Отсутствует'
        elif 'ВК+' in df['Статус'] or 'Выдан в производство' in df['Статус']:
            return 'Утвержден'
        else:
            return 'На согласовании'

    def change_type(self, df:pd.DataFrame) -> str:
        if  pd.isna(df['Тип']):
            return df['Тип_new']
        else:
            return df['Тип']
        
    def change_none(self, df:pd.DataFrame, column:str) -> str:
        if df[column] == 'None' or  df[column] is None:
            return ''
        else:
            return df[column]


        