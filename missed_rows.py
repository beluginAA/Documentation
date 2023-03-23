import pandas as pd

from loguru import logger

# df_1 = doc_database, df_2 = rd_database

class Missed_Rows:

    def __init__(self, df_1, df_2):
        self.df_1 = df_1
        self.df_2 = df_2
    
    def prepare_rows(self, df_1, df_2):

        missed_rows_log = logger.bind(name = 'missed_rows_log').opt(colors = True)
        missed_rows_log.info('Finding information with missing data')

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
        missed_rows = merged2_df[merged2_df['_merge'] == 'right_only'][df_2.columns]

        return missed_rows
    
    def find_rows(self, df_1, df_2):

        system_array = df_1['Система'].drop_duplicates()
        
        print()