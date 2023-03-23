import pandas as pd
import datetime
import pyodbc
import warnings
import main_columns
import sys
import threading

from tkinter.filedialog import askopenfilename
from loguru import logger
from datetime import datetime, date
from log_file import Log_File


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

def thread_function(obj):
    obj.prepare_data(merged1_copy, merged2_copy)
    obj.upload_file(merged1_copy, merged2_copy)
    

logger.remove()
main_file_logger = logger.bind(name = 'main_file_logger').opt(colors = True)
main_file_logger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO")

warnings.simplefilter(action = 'ignore', category = (UserWarning))
database_root = askopenfilename(title = 'Select database to edit', filetypes=[('*.mdb', '*.accdb')]).replace('/', '\\')
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
rd_database = rd_database[list(main_columns.rd_columns)]
rd_database['Ревизия'] = rd_database['Ревизия'].apply(lambda row: None if row == '' else row)
doc_database['Срок'] = doc_database['Срок'].apply(lambda row: row if row in ['в производстве', 'В производстве', None] else datetime.strptime(row, '%d.%m.%Y').date()).astype(str)
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
summary_log_file = Log_File(merged1_copy, merged2_copy)
log_file = threading.Thread(target = thread_function, args = (summary_log_file, ))
log_file.start()

main_file_logger.info('Preparing merging dataframes for summary dataframe')
merged_both1_df = merged1_df[merged1_df['_merge'] == 'both'].copy()
merged_both2_df = merged2_df[merged2_df['_merge'] == 'both'].copy()
merged_both1_df['Тип'] = merged_both1_df.apply(change_type, axis = 1)
merged_both2_df['Тип'] = merged_both2_df.apply(change_type, axis = 1)

main_file_logger.info('Creating a summary table')
dataframe_part = merged2_df[merged2_df['_merge'] == 'left_only'][doc_database.columns]
summary_df = merged_both1_df[list(main_columns.merged_both1_columns)]
summary_df.columns = doc_database.columns
summary_df = pd.concat([dataframe_part, summary_df])

dataframe_part = merged_both2_df.copy()
dataframe_part['Новый шифр'] = dataframe_part.apply(change_code, axis = 1)
dataframe_part = dataframe_part[list(main_columns.merged_both2_columns)]
dataframe_part.columns = doc_database.columns
summary_df = pd.concat([dataframe_part, summary_df])

main_file_logger.info('Preparing the final file and writing it to the database')
summary_df = pd.concat([summary_df, empty_rows_df]).sort_index()
summary_df['Статус'] = summary_df.apply(change_status, axis = 1)

# with pyodbc.connect(conn_str) as connection:
#     cursor = connection.cursor()
#     drop_table_query = '''DROP TABLE [Документация]'''
#     cursor.execute(drop_table_query)
#     cursor.commit()
#     create_table_query = '''CREATE TABLE [Документация] ([Система] VARCHAR(200), 
#                                             [Наименование] VARCHAR(200),
#                                             [Шифр] VARCHAR(100),
#                                             [Разработчик] VARCHAR(200),
#                                             [Вид] VARCHAR(100),
#                                             [Тип] VARCHAR(200),
#                                             [Статус] VARCHAR(200),
#                                             [Ревизия] VARCHAR(200), 
#                                             [Дополнения] VARCHAR(200),
#                                             [Срок] VARCHAR(100),
#                                             [Сервер] VARCHAR(200),
#                                             [Обоснование] VARCHAR(200))'''
#     cursor.execute(create_table_query)
#     cursor.commit()
#     for row in summary_df.itertuples(index=False):
#         insert_query = f'''INSERT INTO [Документация] ([Система], 
#                                             [Наименование],
#                                             [Шифр],
#                                             [Разработчик],
#                                             [Вид],
#                                             [Тип],
#                                             [Статус],
#                                             [Ревизия], 
#                                             [Дополнения],
#                                             [Срок],
#                                             [Сервер],
#                                             [Обоснование]) 
#                                             VALUES ({",".join(f"'{x}'" for x in row)})'''
#         cursor.execute(insert_query)
#     cursor.commit()

main_file_logger.info('Database updated')




