import pandas as pd
import datetime
import pyodbc
import warnings
import Documntation_columns
import sys

from tkinter.filedialog import askopenfilename
from loguru import logger
from datetime import datetime, date


def change_code(df):
    if df['Шифр_new'] != '':
        return df['Шифр_new']
    else:
        return df['Код']

def change_status(df):
    if pd.isna(df['Статус']):
        return 'Отсутствует'
    elif 'ВК+' in df['Статус'] or 'Выдан в производство' in df['Статус']:
        return 'Утвержден'
    else:
        return 'На согласовании'

def change_type(df):
    if pd.isna(df['Тип']):
        return df['Тип_new']
    else:
        return df['Тип'] 
    

logger.remove()
main_file_logger = logger.bind(name = 'main_file_logger').opt(colors = True)
main_file_logger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO")

warnings.simplefilter(action = 'ignore', category = (UserWarning))
# database_root = askopenfilename(title = 'Select database to edit', filetypes=[('*.mdb', '*.accdb')]).replace('/', '\\')
database_root = "C:\\Users\AlABelugin\\Desktop\\Project\\tables_2\\CS_Database_new.accdb"
conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
    fr'DBQ={database_root};'
    )
with pyodbc.connect(conn_str) as connection:
    rd_database_query = '''SELECT * FROM [РД]'''
    doc_database_query = '''SELECT * FROM [Документация]'''
    rd_database = pd.read_sql(rd_database_query, connection)
    doc_database = pd.read_sql(doc_database_query, connection)

main_file_logger.info('Preparing dataframes for merging')
rd_database = rd_database[list(Documntation_columns.rd_columns)]
doc_database['Срок'] = doc_database['Срок'].apply(lambda row: row if row in ['в производстве', 'В производстве', None] else datetime.strptime(row, '%d.%m.%Y').date())
empty_rows_df = doc_database[(pd.isna(doc_database['Шифр'])) | (doc_database['Вид'] != 'Проектная документация') | (doc_database['Разработчик'] != 'Атомэнергопроект')]
doc_database = doc_database[(~pd.isna(doc_database['Шифр'])) & (doc_database['Вид'] == 'Проектная документация') & (doc_database['Разработчик'] == 'Атомэнергопроект')]

main_file_logger.info('Merging two dataframes')
merged1_df = pd.merge(doc_database, rd_database,
                          how = 'left',
                          on = 'Шифр',
                          suffixes=['', '_new'],
                          indicator = True)
left_only = merged1_df[merged1_df['_merge'] == 'left_only'][doc_database.columns]
merged2_df = pd.merge(left_only, rd_database,
                     how = 'left',
                     left_on = 'Шифр',
                     right_on = 'Код',
                     suffixes=['', '_new'],
                     indicator=True)

main_file_logger.info('Making copies of already joined dataframes')
merged1_copy = merged1_df[merged1_df['_merge'] == 'both'].copy()
merged2_copy = merged2_df[merged2_df['_merge'] == 'both'].copy()
# get_log_file(merged1_copy, merged2_copy)

main_file_logger.info('Preparing merging dataframes for summary dataframe')
merged_both1_df = merged1_df[merged1_df['_merge'] == 'both'].copy()
merged_both2_df = merged2_df[merged2_df['_merge'] == 'both'].copy()
merged_both1_df['Тип'] = merged_both1_df.apply(change_type, axis = 1)
merged_both2_df['Тип'] = merged_both2_df.apply(change_type, axis = 1)

main_file_logger.info('Creating a summary table')
dataframe_part = merged2_df[merged2_df['_merge'] == 'left_only'][doc_database.columns]
summary_df = merged_both1_df[list(Documntation_columns.merged_both1_columns)]
summary_df.columns = doc_database.columns
summary_df = pd.concat([dataframe_part, summary_df])

dataframe_part = merged_both2_df.copy()
dataframe_part['Новый шифр'] = dataframe_part.apply(change_code, axis = 1)
dataframe_part = dataframe_part[list(Documntation_columns.merged_both2_columns)]
dataframe_part.columns = doc_database.columns
summary_df = pd.concat([dataframe_part, summary_df])

main_file_logger.info('Preparing the final file and writing it to the database')
summary_df = pd.concat([summary_df, empty_rows_df]).sort_index()
summary_df['Статус'] = summary_df.apply(change_status, axis = 1)
summary_df.to_excel('123.xlsx', index=False)



