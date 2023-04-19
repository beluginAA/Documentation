import pandas as pd
import datetime
import pyodbc
import warnings
import main_columns
import sys
import threading
import traceback


from tkinter.filedialog import askopenfilename
from loguru import logger
from datetime import datetime, date
from log_file import Log_File
from missed_rows import Missed_Rows
from processing import Preproccessing, PostProcessing, ResultFiles
from Functions import Functions
    

logger.remove()
mainFileLogger = logger.bind(name = 'main_file_logger').opt(colors = True)
mainFileLogger.add(sink = sys.stdout, format = "<green> {time:HH:mm:ss} </green> | {message}", level = "INFO", colorize = True)

warnings.simplefilter(action = 'ignore', category = (UserWarning, FutureWarning))

mainFileLogger.info('Program starts.')
databaseRoot = askopenfilename(title = 'Select database to edit', filetypes=[('*.mdb', '*.accdb')]).replace('/', '\\')

mainFileLogger.info('Preparing dataframes for merging.')
func = Functions()
connect = Preproccessing(databaseRoot)
result = ResultFiles()
rdDatabase, docDatabase = connect.to_database()
rdDatabase = rdDatabase[list(main_columns.rdColumns)]
for col in main_columns.rdColumns:
    rdDatabase[col] = rdDatabase.apply(lambda df: func.finding_empty_rows(df, col), axis = 1)
for col in main_columns.doc_columns:
    docDatabase[col] = docDatabase.apply(lambda df: func.finding_empty_rows(df, col), axis = 1)
rdDatabase['Ревизия'] = rdDatabase['Ревизия'].apply(lambda row: None if row == '' else row)
docDatabase['Срок'] = docDatabase['Срок'].apply(lambda row: row if row in ['в производстве', 'В производстве', None] else datetime.strptime(row, '%d.%m.%Y').date().strftime('%d-%m-%Y'))
empty_rows_df = docDatabase[(pd.isna(docDatabase['Шифр'])) | (docDatabase['Вид'] != 'Проектная документация') | (docDatabase['Разработчик'] != 'Атомэнергопроект')]
docDatabase = docDatabase[(~pd.isna(docDatabase['Шифр'])) & (docDatabase['Вид'] == 'Проектная документация') & (docDatabase['Разработчик'] == 'Атомэнергопроект')]

mainFileLogger.info('Making copy of original dataframes.')
docDatabaseCopy = docDatabase.copy()
rdDatabaseCopy = rdDatabase.copy()
missedRows = Missed_Rows()
missedDf = threading.Thread(target = func.thread_missed_function, args = (missedRows, result, docDatabaseCopy, rdDatabaseCopy,))
missedDf.start()

mainFileLogger.info('Merging two dataframes.')
cipherDf = pd.merge(docDatabase, rdDatabase,
                          how = 'left',
                          on = 'Шифр',
                          suffixes=['', '_new'],
                          indicator = True)
leftOnly = cipherDf[cipherDf['_merge'] == 'left_only'][docDatabase.columns]
cipherCodeDf = pd.merge(leftOnly, rdDatabase,
                     how = 'left',
                     left_on = 'Шифр',
                     right_on = 'Код',
                     suffixes=['', '_new'],
                     indicator=True)

mainFileLogger.info('Making copies of already joined dataframes.')
cipherDf = cipherDf[cipherDf['_merge'] == 'both'].copy()
cipherCodeDfCopy = cipherCodeDf[cipherCodeDf['_merge'] == 'both'].copy()
summaryLogFile = Log_File(cipherDf, cipherCodeDfCopy)
logFile = threading.Thread(target = func.thread_log_function, args = (summaryLogFile, result, cipherDf, cipherCodeDfCopy,))
logFile.start()

mainFileLogger.info('Preparing merging dataframes for summary dataframe.')
resultCipherDf = cipherDf[cipherDf['_merge'] == 'both'].copy()
resultCipherCodeDf = cipherCodeDf[cipherCodeDf['_merge'] == 'both'].copy()
resultCipherDf['Тип'] = resultCipherDf.apply(func.change_type, axis = 1)
resultCipherCodeDf['Тип'] = resultCipherCodeDf.apply(func.change_type, axis = 1)

mainFileLogger.info('Creating a summary table.')
partDf = cipherCodeDf[cipherCodeDf['_merge'] == 'left_only'][docDatabase.columns]
summaryDf = resultCipherDf[list(main_columns.CipherDfColumns)]
summaryDf.columns = docDatabase.columns
summaryDf = pd.concat([partDf, summaryDf])

partDf = resultCipherCodeDf.copy()
partDf['Новый шифр'] = partDf.apply(func.change_code, axis = 1)
partDf = partDf[list(main_columns.CipherCodeDfColumns)]
partDf.columns = docDatabase.columns
summaryDf = pd.concat([partDf, summaryDf])

mainFileLogger.info('Preparing the final file and writing it to the database.')
summaryDf = pd.concat([summaryDf, empty_rows_df]).sort_index()
summaryDf['Статус'] = summaryDf.apply(func.change_status, axis = 1)
for column in main_columns.noneColumns:
    summaryDf[column] = summaryDf.apply(lambda df: func.change_none(df, column), axis = 1)

# mainFileLogger.info('Making changes to the database.')
# attempt = PostProcessing(databaseRoot)
# attempt.delete_table()
# attempt.create_table()
# attempt.insert_into_table()

# mainFileLogger.info('Database updated.')




