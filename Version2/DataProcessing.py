import pandas as pd
import numpy as np
import pandas_ta as ta
import logging

# NOTES: We are currenlty assuming that all values are present in the excel file:
# the data is continous. TO ADD: check for discontnous data and find a replacement strategy.


class ETL:
    def __init__(self):
        print('created')

    def Load_Clean_Data(self, input_file_path, interval='1d'):
        try:
            # second row is column names
            df = pd.read_csv(input_file_path, header=1)
            col_list = ['unix_start_datetime', 'start_date', 'symbol', 'open',
                        'high', 'low', 'close', 'base_volume', 'quote_volume', 'num_trades']
            df.columns = col_list
            df["interval"] = interval
            df.dropna(subset=['symbol', 'unix_start_datetime', 'open','close', 'high', 'low', 'base_volume'], inplace=True)
            return df.copy()
        except Exception as e:
            logging.error('Failed to load and clean data: \n' + str(e))

    def Add_Label_TI_Data(self, df, window_size=5, alpha=0.005, k=14, d=3):
        try:
            # add technical indicators
            # adds 2 columns "STOCHk_14_3_3","STOCHd_14_3_3"
            df.ta.stoch(high='high', low='low', close='close',k=k, d=d, fillna=True, append=True)
            MACD = ta.macd(df['close'])
            df['MACD'] = MACD['MACD_12_26_9']
            df['MACDs'] = MACD['MACDs_12_26_9']
            df = df.rename(columns={"STOCHk_14_3_3": "STOCHk", "STOCHd_14_3_3": "STOCHd"})
            #Normalize data
            norm_kd=[0,0]
            norm_macd=[0,0]
            for index, row in df.iloc[2:].iterrows():
                # KD line normalization
                if (df.at[index - 1,'STOCHk'] < df.at[index - 1,'STOCHd'] and row['STOCHk'] < row['STOCHd']):
                    if (20 >= df.at[index - 2,'STOCHk'] and 20 >= df.at[index - 1,'STOCHk'] and 25 <= row['STOCHk']):
                        norm_kd.append(1.5)
                    elif (20 < df.at[index - 2,'STOCHk'] < 80 or 20 < df.at[index - 1,'STOCHk'] < 80 or 20 < row['STOCHk'] < 80):
                        norm_kd.append(1)
                    else:
                        norm_kd.append(0)
                elif (df.at[index - 1,'STOCHk'] > df.at[index - 1,'STOCHd'] and row['STOCHk'] > row['STOCHd']):
                    if (80 <= df.at[index - 2,'STOCHk'] and 80 <= df.at[index - 1,'STOCHk'] and 75 >= row['STOCHk']):
                        norm_kd.append(-1.5)
                    elif (20 < df.at[index - 1,'STOCHk'] < 80 or 20 < df.at[index - 1,'STOCHk'] < 80 or 20 < row['STOCHk'] < 80):
                        norm_kd.append(-1)
                    else:
                        norm_kd.append(0)
                else:
                    norm_kd.append(0)

                # MACD Norlmalization
                if (df.at[index - 1,'MACD'] < df.at[index - 1,'MACDs'] and row['MACDs'] < row['MACD']):
                    norm_macd.append(1)
                elif (df.at[index - 1,'MACD'] > df.at[index - 1,'MACDs'] and row['MACDs'] > row['MACD']):
                    norm_macd.append(-1)
                else:
                    norm_macd.append(0)
            # print(df.shape[0])
            # print(len(norm_kd))
            # print(len(norm_macd))
            df['ti_kd']= norm_kd
            df['ti_MACD']= norm_macd
        except Exception as e:
            logging.ERROR('Failed to add TI data: \n' + str(e))

        try:
            #Create Labels: 2 types of label, a) short term b) daily
            ma_minus = df['close'].rolling(window=window_size).mean()
            ma_plus = ma_minus.shift(-(window_size-1))
            n=5
            resultA = []
            resultB = []
            for i in range(ma_plus.shape[0]):
                #NOTE: bull, bear flat or buy sell hold
                #TODO: what
                tempA = "up" if (ma_plus[i]-ma_minus[i]) > (ma_minus[i]*alpha) else "down" if (ma_plus[i]-ma_minus[i]) < (-ma_minus[i]*alpha) else "flat"
                resultA.append(tempA)
                if(i==(ma_plus.shape[0] -1)):
                    resultB.append('flat')
                else:
                    tempB= "up" if (df.loc[i+1,"close"]- df.loc[i,"close"]) > (df.loc[i,"close"]*alpha) else "down" if (df.loc[i+1,"close"]-  df.loc[i,"close"]) < (-df.loc[i,"close"]*alpha) else "flat"
                    resultB.append(tempB)
            df['result_A'] = resultA
            df['result_B'] = resultB
            #Drop all NA value
            df = df.drop(range(33)) #TODO: make dynamic
            return df
        except Exception as e:
            logging.error('Failed to add labels to data: \n' + str(e))
            return
