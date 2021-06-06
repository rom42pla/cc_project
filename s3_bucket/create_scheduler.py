from datetime import time
import pandas as pd


def all_timestamps(file):
    timestamps = pd.read_csv(file,usecols=[0])
    timestamps['day']=timestamps['event_time'].apply(lambda x: x.split(' ')[0])
    days_list = timestamps['day'].unique().tolist()
    return days_list


def __main__():
    days = all_timestamps('RS_data.csv')
    with open('days_scheduler.txt', mode='wt', encoding='utf-8') as myfile:
        myfile.write('\n'.join(days))
    return 1

__main__()