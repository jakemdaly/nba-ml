import sys, copy, time
from numpy.lib.function_base import average
sys.path.append(r"/home/jakemdaly/Documents/GitRepos/nba-ml")

import torch
from torch import nn, optim

import pymongo
from pymongo import MongoClient
import numpy as np
import math

import pandas as pd
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import random
import matplotlib.pyplot as plt
from joblib import dump
from icecream import ic
from pymongo import MongoClient

from nba_api.stats.static.players import get_players, get_active_players

from db.mongo.fetch_utils import get_all_player_ids, get_all_game_ids, get_all_team_ids
from db.mongo.populate_utils import *
from db.mongo.enums import *
from db.mongo.utils import add_age_to_player_season
from nba.utils import get_fantasy_points_from_game
from nba.dataset_building_utilities import create_player_season_dataset

# add_age_to_player_season()
# create_player_season_dataset()

nba_db = client['NBA']
dataset_db = client['Datasets']
player_lookup = {doc['PLAYER_ID']: doc['PLAYER_NAME'] for doc in nba_db.Players.find({})}
active_player_names = [plyr['PLAYER_NAME'] for plyr in nba_db.Players.find({}) if plyr['IS_ACTIVE']==True]

trim_features = lambda data: data.drop(columns=['PLAYER_NAME','_id', 'PLAYER_ID','SEASON', 'FANTASY_POINTS_v0']).to_numpy()#, 'OFF_RATING', 'DEF_RATING', 'NET_RATING', 'AST_PCT', 'OREB_PCT', 'DREB_PCT', 'EFG_PCT', 'TS_PCT', 'USG_PCT', 'PACE']).to_numpy()

class SeasonDataset(Dataset):

    def __init__(self, data):
        """
        Args:
            data: list of mongo docs of type DatasetPlayerSeason
        """

        self.data = pd.DataFrame(data)
        self.pids = list(self.data.PLAYER_ID.unique()) # player_ids
        self.transform = StandardScaler()

    def __len__(self):
        # return number of sensors
        return len(self.pids)

    # Will pull an index between 0 and __len__. 
    def __getitem__(self, idx):

        
        pid = self.pids[idx]
        player = player_lookup[pid]
        data = self.data[self.data.PLAYER_ID==pid]
        targets = data.FANTASY_POINTS_v0.to_numpy()
        data = trim_features(data)

        # # scalar is fit only to the input, to avoid the scaled values "leaking" information about the target range.
        # # scalar is fit only for humidity, as the timestamps are already scaled
        # # scalar input/output of shape: [n_samples, n_features].
        # scaler = self.transform

        # scaler.fit(_input)
        # _input = torch.tensor(scaler.transform(_input)) # 
        # target = torch.tensor(scaler.transform(target))

        # # save the scalar to be used later when inverse translating the data for plotting.
        # dump(scaler, 'scalar_item.joblib')

        return data, targets, player
    
    def balanced_sample(self):

        BUCKET_SIZE = 5

        def round_down(num, divisor):
            return num - (num%divisor)

        def round_up(num, divisor):
            return num + (divisor-num%divisor)

        ave = self.data.groupby(['PLAYER_ID', 'PLAYER_NAME'], as_index=False).max()[['PLAYER_ID', 'FANTASY_POINTS_v0']].copy()
        ave = ave.rename(columns={'FANTASY_POINTS_v0':'FP_MAX'}).sort_values('FP_MAX', ascending=False)
        max_val = round_up(ave['FP_MAX'].max(), BUCKET_SIZE)
        min_val = round_down(ave['FP_MAX'].min(), BUCKET_SIZE)
        bucket_keys = [n for n in range(1, int((max_val - min_val)/BUCKET_SIZE)+1)]
        buckets = {n: None for n in bucket_keys}
        for key in bucket_keys:
            ids = ave[ave.FP_MAX <= key*BUCKET_SIZE+min_val].PLAYER_ID.to_list()
            buckets[key] = ids
        for _ in range(len(self)):

            bucket = random.randint(1,8)
            pid = random.choice(buckets[bucket])

            player = player_lookup[pid]
            data = self.data[self.data.PLAYER_ID==pid]
            targets = data.FANTASY_POINTS_v0.to_numpy()
            data = trim_features(data)

            data = torch.tensor(data, dtype=torch.double).unsqueeze(0)
            targets = torch.tensor(targets, dtype=torch.double)

            yield data, targets, player


class RNN(nn.Module):

    def __init__(self, embedding_dim, hidden_dim):
        super(RNN, self).__init__()
        self.hidden_dim = hidden_dim

        self.lstm = nn.LSTM(embedding_dim, hidden_dim, num_layers=2, bias=True, dropout=.5)
        self.linear1 = nn.Linear(hidden_dim, out_features=32, bias=True)
        self.linear2 = nn.Linear(32, out_features=8, bias=True)
        self.relu   = nn.ReLU()
        self.linear_out = nn.Linear(bias=True, in_features=8, out_features=1)
        
    def forward(self, sequence):
        lstm_out, _ = self.lstm(sequence.double())
        h = self.linear1(lstm_out.view(sequence.shape[1], -1))
        h = self.relu(h)
        h = self.linear2(h)
        h = self.relu(h)
        out = self.linear_out(h)
        return out

EPOCHS = 48
lr=0.0001
EMBEDDING_DIM = 30
HIDDEN_DIM = 64

player_ids = dataset_db.DatasetPlayerSeason.distinct("PLAYER_ID")
random.shuffle(player_ids)
N_PLAYERS  = len(player_ids)
TRAIN_SIZE = int(N_PLAYERS * .8)
train_ids  = player_ids[:TRAIN_SIZE]
test_ids   = player_ids[TRAIN_SIZE:]

train_data = [doc for doc in dataset_db.DatasetPlayerSeason.find({"PLAYER_ID": {"$in": train_ids}})]
dataset_train = SeasonDataset(train_data)
# dataset_train.set_balanced_sample()
dataset_train_eval = DataLoader(dataset_train, batch_size=1, shuffle=True)

test_data = [doc for doc in dataset_db.DatasetPlayerSeason.find({"PLAYER_ID": {"$in": test_ids}})]
dataset_test = SeasonDataset(test_data)
dataset_test_eval = DataLoader(dataset_test, batch_size=1, shuffle=True)


model = RNN(EMBEDDING_DIM, HIDDEN_DIM)
model = model.double()
loss_function = nn.MSELoss()
optimizer = optim.Adam(model.parameters(), lr=lr)

player_ordering = dict()

for epoch in range(EPOCHS):
    train_loss = 0
    with tqdm(total=TRAIN_SIZE) as pbar:
        for sequence, targets, player in dataset_train.balanced_sample(): # sequence is a list of a player's seasons in order

            model.zero_grad()
            pred = model(sequence)
            loss = loss_function(pred.double(), targets.reshape(pred.shape[0], -1).double())
            train_loss += loss.detach().item()
            loss.backward()
            optimizer.step()
            pbar.update()

    print("="*30)
    print(f"Epoch {epoch:<3} | Ave. Train Loss {train_loss/len(train_data):.2f}")
    
    test_loss = 0
    with torch.no_grad():
        for sequence, targets, player in dataset_test_eval:
            pred = model(sequence)
            loss = loss_function(pred.double(), targets.reshape(pred.shape[0], -1).double())
            test_loss += loss.detach().item()

    print(f"Epoch {epoch:<3} | Ave. Test Loss {test_loss/len(test_data):.2f}")
    print("="*30)
    
    if (epoch+1)%8 == 0:

        PREDS=[]
        with torch.no_grad():
            for sequence, targets, player in dataset_train_eval:
                if player[0] in active_player_names:
                    pred = model(sequence)
                    PREDS.append({'Player': player[0], 'Pred': pred[-1,-1].item()})
            for sequence, targets, player in dataset_test_eval:
                if player[0] in active_player_names:
                    pred = model(sequence)
                    PREDS.append({'Player': player[0], 'Pred': pred[-1,-1].item()})
            # pd.DataFrame(PREDS).sort_values('Pred', ascending=False).to_excel(f"Predictions-{EPOCHS}e-{lr}lr-{HIDDEN_DIM}hdim.xlsx")
            df = pd.DataFrame(PREDS).sort_values('Pred', ascending=False).reset_index(drop=True)
            df.to_excel(f"Epoch{epoch}.xlsx")
            for index, row in df.iterrows():
                if row.Player not in player_ordering:
                    player_ordering[row.Player] = [index]
                else:
                    player_ordering[row.Player] += [index]

average_orderings = []
for player, orderings in player_ordering.items():
    average_orderings.append({'Player': player, 'Avg. Position': np.mean(orderings)})
    df = pd.DataFrame(average_orderings).sort_values('Avg. Position', ascending=True).reset_index(drop=True)
    df.to_excel(f"AverageOrdering.xlsx")

exit()


