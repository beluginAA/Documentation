import pandas as pd
import pyodbc
import sys
import pyxlsb
import os
import colorama
import xlsxwriter

from loguru import logger
from datetime import datetime


class Preproccessing:

    preLogger = logger.bind(name = 'preLogger').opt(colors = True)
    preLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

    def __init__(self, databaseRoot:str):
        self.connStr = (
            r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
            fr'DBQ={databaseRoot};'
            )

    def to_database(self) -> pd.DataFrame :
        Preproccessing.preLogger.info('Trying to connect to a database.')
        try:
            with pyodbc.connect(self.connStr) as connection:
                rdDatabaseQuery = '''SELECT * FROM [РД]'''
                docDatabaseQuery = '''SELECT * FROM [Документация]'''
                rdDatabase = pd.read_sql(rdDatabaseQuery, connection)
                docDatabase = pd.read_sql(docDatabaseQuery, connection)
        except Exception as e:
            Preproccessing.preLogger.error(f"An error occurred while connecting to the database: {e}")
        else:
            Preproccessing.preLogger.info('--The connection to the database was successful.--')
            return rdDatabase, docDatabase
        


class PostProcessing:

    postLogger = logger.bind(name = 'postLogger').opt(colors = True)
    postLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

    def __init__(self, databaseRoot:str):
        self.connStr = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                fr'DBQ={databaseRoot};'
                )

    def delete_table(self) -> None:
        PostProcessing.postLogger.info('Trying to delete an old table.')
        try:
            with pyodbc.connect(self.connStr) as connection:
                cursor = connection.cursor()
                cursor.execute(f"DROP TABLE [Документация]")
                cursor.commit()
        except Exception as e:
            PostProcessing.postLogger.error(f"An error occurred while deleting the table: {e}")
        else:
            PostProcessing.postLogger.info('--An old table has been successfully deleted.--')
    
    def create_table(self) -> None:
        PostProcessing.postLogger.info('Trying to create a new table.')
        createTableQuery = '''CREATE TABLE [Документация] ([Система] VARCHAR(200), 
                                            [Наименование] VARCHAR(200),
                                            [Шифр] VARCHAR(100),
                                            [Разработчик] VARCHAR(200),
                                            [Вид] VARCHAR(100),
                                            [Тип] VARCHAR(200),
                                            [Статус] VARCHAR(200),
                                            [Ревизия] VARCHAR(200), 
                                            [Дополнения] VARCHAR(200),
                                            [Срок] VARCHAR(100),
                                            [Сервер] VARCHAR(200),
                                            [Обоснование] VARCHAR(200))'''
        try:
            with pyodbc.connect(self.connStr) as connection:
                cursor = connection.cursor()
                cursor.execute(createTableQuery)
                cursor.commit()
        except Exception as e:
            PostProcessing.postLogger.error(f"An error occurred while creating the table: {e}")
        else:
            PostProcessing.postLogger.info('--An old table has been successfully created.--')
    
    def insert_into_table(self, dataframe:pd.DataFrame) -> None:
        PostProcessing.postLogger.info('Trying to insert new data into new table.')
        try:
            with pyodbc.connect(self.connStr) as connection:
                cursor = connection.cursor()
                for row in dataframe.itertuples(index=False):
                    insertQuery = f'''INSERT INTO [Документация] ([Система], [Наименование],
                                            [Шифр], [Разработчик],
                                            [Вид], [Тип],
                                            [Статус], [Ревизия], 
                                            [Дополнения], [Срок],
                                            [Сервер], [Обоснование]) 
                                            VALUES ({",".join(f"'{x}'" for x in row)})'''
                    cursor.execute(insertQuery)
                cursor.commit()
        except Exception as e:
            PostProcessing.postLogger.error(f"An error occurred while inserting the data: {e}")
        else:
            PostProcessing.postLogger.info('--Data was successfully added to the table.--')



class ResultFiles:

    resultFileLogger = logger.bind(name = 'resultFileLogger').opt(colors = True)
    resultFileLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

    def __init__(self):
        self.outputLogLileName = 'log-RD-' + str(datetime.now().isoformat(timespec='minutes')).replace(':', '_')
        self.outputResultFileName = 'missedRows' + str(datetime.now().isoformat(timespec='minutes')).replace(':', '_')
    
    def to_logfile(self, dataframe:pd.DataFrame, header:str) -> None:
        ResultFiles.resultFileLogger.info('Trying to write data to log-file.')

        try:
            maxLenRow = [max(dataframe[row].apply(lambda x: len(str(x)) if x else 0)) for row in dataframe.columns]
            maxLenName = [len(row) for row in dataframe.columns]
            maxLen = [col_len if col_len > row_len else row_len for col_len, row_len in zip(maxLenName, maxLenRow)]
            with open(f'{self.outputLogLileName}.txt', 'a',  encoding='cp1251') as logFile:
                logFile.write(f'{header}:\n')
                logFile.write('\n')
                fileWrite = ' ' * (len(str(dataframe.index.max())) + 3)
                for column, col_len in zip(dataframe.columns, maxLen):
                    fileWrite += f"{column:<{col_len}}|"
                logFile.write(fileWrite)
                logFile.write('\n')
                for index, row in dataframe.iterrows():
                    columnValue = ''
                    for i in range(len(dataframe.columns)):
                        columnValue += f"{str(row[dataframe.columns[i]]) if row[dataframe.columns[i]] else '-':<{maxLen[i]}}|"
                    logFile.write(f"{index: <{len(str(dataframe.index.max()))}} | {columnValue}\n")
                logFile.write('\n')
        except Exception as e:
            ResultFiles.resultFileLogger.error(f"An error occurred while writing data to log-file: {e}")
        else:
            ResultFiles.resultFileLogger.info('--Writing data to log-file was successful.--')
    
    def to_resultfile(self, dataframe:pd.DataFrame) -> None:
        ResultFiles.resultFileLogger.info('Trying to write the missed rows to an excel file.')

        try:
            dataframe.to_excel(f'./{self.outputResultFileName}.xlsx', index = False)
            styler = dataframe.style
            styler.set_properties(**{'border': '1px solid black', 'border-collapse': 'collapse'})
            writer = pd.ExcelWriter(f'./{self.outputResultFileName}.xlsx', engine='xlsxwriter')
            styler.to_excel(writer, sheet_name='Пропущенные значения', encoding='cp1251', index=False)
            workbook = writer.book
            worksheet = writer.sheets['Пропущенные значения']
            worksheet.autofilter(0, 0, len(dataframe.index), len(dataframe.columns) - 1)
            for i, column in enumerate(dataframe.columns):
                column_width = max(dataframe[column].astype(str).map(len).max(), len(column))
                worksheet.set_column(i, i, column_width)
            writer._save()
        except Exception as e:
            ResultFiles.resultFileLogger.error(f"An error occurred while writing the missed rows to an excel file: {e}")
        else:
            ResultFiles.resultFileLogger.info('Writing missed rows to excel file was successful.')
