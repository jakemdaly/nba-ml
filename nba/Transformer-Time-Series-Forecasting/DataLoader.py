import pandas as pd
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import os
import torch
import numpy as np
import random
import matplotlib.pyplot as plt
from joblib import dump
from icecream import ic
from pymongo import MongoClient


client = MongoClient('localhost', 27017)
nba_db = client['NBA']
player_lookup = {doc['PLAYER_ID']: doc['PLAYER_NAME'] for doc in nba_db.Players.find({})}

class GameLogDataset(Dataset):

    def __init__(self, training_length, forecast_window, data):
        """
        Args:
            csv_file (string): Path to the csv file.
            root_dir (string): Directory
        """

        self.data = data
        
        self.transform = StandardScaler()
        # self.T = training_length
        self.S = forecast_window

    def __len__(self):
        # return number of sensors
        return len(self.data)

    # Will pull an index between 0 and __len__. 
    def __getitem__(self, idx):

        # start = np.random.randint(0, len(self.data[idx]['data']) - self.T - self.S) 
        start = 0
        player = player_lookup[self.data[idx]['_id']]
        train_length = len(self.data[idx]['data']) - self.S
        index_in = torch.tensor([i for i in range(start, train_length)])
        index_tar = torch.tensor([i for i in range(start + train_length, start + train_length + self.S)])
        _input = torch.tensor(np.nan_to_num(self.data[idx]['data']))[start:start+train_length]
        _input = torch.roll(_input, 2, 1) # move fpoints to beginning index
        target = torch.tensor(np.nan_to_num(self.data[idx]['data']))[start+train_length:start+train_length+self.S]
        target = torch.roll(target, 2, 1) # move fpoints to beginning index

        # scalar is fit only to the input, to avoid the scaled values "leaking" information about the target range.
        # scalar is fit only for humidity, as the timestamps are already scaled
        # scalar input/output of shape: [n_samples, n_features].
        # scaler = self.transform

        # scaler.fit(_input)
        # _input = torch.tensor(scaler.transform(_input)) # 
        # target = torch.tensor(scaler.transform(target))

        # # save the scalar to be used later when inverse translating the data for plotting.
        # dump(scaler, 'scalar_item.joblib')

        return index_in, index_tar, _input, target, player
    
    def balanced_sample(self):

        BUCKET_SIZE = 5

        self.data = pd.DataFrame(self.data)

        def round_down(num, divisor):
            return num - (num%divisor)
        
        def round_up(num, divisor):
            return num + (divisor-num%divisor)

        ave = [{'pid': item['_id'], 'ave': torch.nan_to_num(torch.roll(torch.tensor(item['data']),2,1)).mean(dim=0)[0].item()} for i, item in self.data.iterrows()]
        # ave = ave.rename(columns={'FANTASY_POINTS_v0':'FP_MAX'}).sort_values('FP_MAX', ascending=False)
        ave = pd.DataFrame(ave)
        max_val = round_up(ave['ave'].max(), BUCKET_SIZE)
        min_val = round_down(ave['ave'].min(), BUCKET_SIZE)
        bucket_keys = [n for n in range(1, int((max_val - min_val)/BUCKET_SIZE)+1)]
        buckets = {n: None for n in bucket_keys}
        for key in bucket_keys:
            ids = ave[ave.ave <= key*BUCKET_SIZE+min_val].pid.to_list()
            buckets[key] = ids
        for _ in range(len(self)):

            bucket = random.randint(1,len(bucket_keys))
            pid = random.choice(buckets[bucket])

            player = player_lookup[pid]
            data = torch.nan_to_num(torch.roll(torch.tensor(self.data[self.data._id==pid].data.item()),2,1))

            start = 0
            train_length = len(data) - self.S
            index_in = torch.tensor([i for i in range(start, train_length)])
            index_tar = torch.tensor([i for i in range(start + train_length, start + train_length + self.S)])
            
            _input = data[start:start+train_length]
            target = data[start+train_length:start+train_length+self.S]

            yield index_in, index_tar, _input.unsqueeze(0), target.unsqueeze(0), player


class SensorDataset(Dataset):
    """Face Landmarks dataset."""

    def __init__(self, csv_name, root_dir, training_length, forecast_window):
        """
        Args:
            csv_file (string): Path to the csv file.
            root_dir (string): Directory
        """
        
        # load raw data file
        csv_file = os.path.join(root_dir, csv_name)
        self.df = pd.read_csv(csv_file)
        self.root_dir = root_dir
        self.transform = MinMaxScaler()
        self.T = training_length
        self.S = forecast_window

    def __len__(self):
        # return number of sensors
        return len(self.df.groupby(by=["reindexed_id"]))

    # Will pull an index between 0 and __len__. 
    def __getitem__(self, idx):
        
        # Sensors are indexed from 1
        idx = idx+1

        # np.random.seed(0)

        start = np.random.randint(0, len(self.df[self.df["reindexed_id"]==idx]) - self.T - self.S) 
        sensor_number = str(self.df[self.df["reindexed_id"]==idx][["sensor_id"]][start:start+1].values.item())
        index_in = torch.tensor([i for i in range(start, start+self.T)])
        index_tar = torch.tensor([i for i in range(start + self.T, start + self.T + self.S)])
        _input = torch.tensor(self.df[self.df["reindexed_id"]==idx][["humidity", "sin_hour", "cos_hour", "sin_day", "cos_day", "sin_month", "cos_month"]][start : start + self.T].values)
        target = torch.tensor(self.df[self.df["reindexed_id"]==idx][["humidity", "sin_hour", "cos_hour", "sin_day", "cos_day", "sin_month", "cos_month"]][start + self.T : start + self.T + self.S].values)

        # scalar is fit only to the input, to avoid the scaled values "leaking" information about the target range.
        # scalar is fit only for humidity, as the timestamps are already scaled
        # scalar input/output of shape: [n_samples, n_features].
        scaler = self.transform

        scaler.fit(_input[:,0].unsqueeze(-1))
        _input[:,0] = torch.tensor(scaler.transform(_input[:,0].unsqueeze(-1)).squeeze(-1)) # 
        target[:,0] = torch.tensor(scaler.transform(target[:,0].unsqueeze(-1)).squeeze(-1))

        # save the scalar to be used later when inverse translating the data for plotting.
        dump(scaler, 'scalar_item.joblib')

        #      (48)         (24)     (48,7)  (24,7)   
        return index_in, index_tar, _input, target, sensor_number


