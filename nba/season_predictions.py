import sys, copy, time
sys.path.append(r"/home/jakemdaly/Documents/GitRepos/nba-ml")

import torch
from torch import nn
import pymongo
from pymongo import MongoClient
import numpy as np
import math

from db.mongo.fetch_utils import get_all_player_ids, get_all_game_ids, get_all_team_ids
from db.mongo.populate_utils import *
from db.mongo.enums import *
from nba.utils import get_fantasy_points_from_game

import logging

logging.basicConfig(filename='MyLog.log',level=logging.DEBUG)

nba_db = client['NBA']
dataset = nba_db.DatasetPlayerGameLogs
ids = dataset.distinct("PLAYER_ID")

player_lookup = {doc['PLAYER_ID']: doc['PLAYER_NAME'] for doc in nba_db.Players.find({})}

data = [doc for doc in nba_db.DatasetTransformer.find({})]

# y = input("Are you sure you want to run this really long piece of code? Data set might already be cached in mongo")
# if y=='y' or y=='Y':
#     pass
# else:
#     exit()


# for _id in tqdm(ids):
#     # Get all the game logs for this player
#     gls = [doc for doc in dataset.find({"PLAYER_ID":_id}).sort("GAME_DATE", pymongo.ASCENDING)]
#     vecs = []
#     # add some additional data, and remove some unneeded ones
#     for i, gl in enumerate(gls):
#         if i==0:
#             gl['DAYS_SINCE_LAST_GAME'] = -1
#         else:
#             gl['DAYS_SINCE_LAST_GAME'] = (gls[i]['GAME_DATE'] - date).days
#         gl.pop('PLAYER_ID')
#         gl.pop('PLAYER_NAME')
#         date = gl.pop('GAME_DATE')
#         gl.pop('GAME_ID')
#         gl.pop('_id')
#         break
#     break

#         vecs.append(list(gl.values()))
        
#     nba_db.DatasetTransformer.insert_one({"_id": _id, "data":vecs})

def encode_vector(vector):
    categories = ['MIN', 'OFF_RATING', 'DEF_RATING', 'NET_RATING', 'AST_PCT', 'OREB_PCT', 'DREB_PCT', 'EFG_PCT', 'TS_PCT', 'USG_PCT', 'PACE', 'POSS', 'PIE', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS', 'PLAYER_AGE', 'START_F', 'START_C', 'START_G', 'FANTASY_POINTS_v0', 'DAYS_SINCE_LAST_GAME']
    encoding = dict()
    for vec_el, category in zip(vector, categories):
        encoding[category] = vec_el

    return encoding

class TorchStandardScaler:
    def fit(self, x):
        x = np.nan_to_num(x)
        self.mean = x.mean(0)
        self.std = x.std(0)
    def transform(self, x):
        x -= self.mean
        x /= (self.std + 1e-7)
        return x
  

class Time2Vec(nn.Module):
    def __init__(self, input_shape, kernel_size=1):
        super(Time2Vec, self).__init__()
        self.k = kernel_size

        self.wb = torch.nn.Parameter(torch.rand(input_shape[1]))
        self.bb = torch.nn.Parameter(torch.rand(input_shape[1]))
        # periodic
        self.wa = torch.nn.Parameter(torch.rand((1, input_shape[1], self.k)))
        self.ba = torch.nn.Parameter(torch.rand((1, input_shape[1], self.k)))
    
    def forward(self, inputs):
        bias = self.wb * inputs + self.bb
        dp = torch.dot(inputs, self.wa) + self.ba
        wgts = torch.sin(dp) 

        ret = torch.cat([bias.unsqueeze(), wgts], -1)
        ret = torch.reshape(ret, (-1, inputs.shape[1]*(self.k+1)))
        return ret
    
    def compute_output_shape(self, input_shape):
        return (input_shape[0], input_shape[1]*(self.k + 1))

class AttentionBlock(nn.Module):
    def __init__(self, input_shape, num_heads=2, head_size=128, ff_dim=None, dropout=0):
        super().__init__()

        if ff_dim is None:
            ff_dim = head_size

        self.attention = nn.MultiheadAttention(embed_dim=head_size, num_heads=num_heads, dropout=dropout)
        self.attention_dropout = nn.Dropout(dropout)
        self.attention_norm = nn.LayerNorm(head_size, eps=1e-6)

        self.ff_conv1 = nn.Conv1d(in_channels=head_size, out_channels=ff_dim, kernel_size=1)
        self.activation = nn.ReLU()

        self.ff_conv2 = nn.Conv1d(in_channels=ff_dim, out_channels=input_shape[-1], kernel_size=1) 
        self.ff_dropout = nn.Dropout(dropout)

        self.ff_norm = nn.LayerNorm(input_shape[-1], eps=1e-6)


    def forward(self, inputs):
        x = self.attention([inputs, inputs])
        x = self.attention_dropout(x)
        x = self.attention_norm(inputs + x)

        x = self.ff_conv1(x)
        x = self.activation(x)
        x = self.ff_conv2(x)
        x = self.ff_dropout(x)

        x = self.ff_norm(inputs + x)
        return x

class TimeDistributed(nn.Module):
    def __init__(self, module, batch_first=False):
        super(TimeDistributed, self).__init__()
        self.module = module
        self.batch_first = batch_first

    def forward(self, x):

        if len(x.size()) <= 2:
            return self.module(x)

        # Squash samples and timesteps into a single axis
        x_reshape = x.contiguous().view(-1, x.size(-1))  # (samples * timesteps, input_size)

        y = self.module(x_reshape)

        # We have to reshape Y
        if self.batch_first:
            y = y.contiguous().view(x.size(0), -1, y.size(-1))  # (samples, timesteps, output_size)
        else:
            y = y.view(-1, x.size(1), y.size(-1))  # (timesteps, samples, output_size)

        return y

class Transformer(nn.Module):
    def __init__(self, input_shape=(35,), time2vec_dim=1, num_heads=2, head_size=128, ff_dim=None, num_layers=1, dropout=0, **kwargs):
        super().__init__()
        self.time2vec = Time2Vec(input_shape=input_shape, kernel_size=time2vec_dim)
        if ff_dim is None:
            ff_dim = head_size
        self.dropout = dropout
        self.attention_layers = [AttentionBlock(input_shape=input_shape, num_heads=num_heads, head_size=head_size, ff_dim=ff_dim, dropout=dropout) for _ in range(num_layers)]

        
    def forward(self, inputs):
        time_embedding = TimeDistributed(self.time2vec)(inputs)
        x = torch.cat([inputs, time_embedding], -1)
        for attention_layer in self.attention_layers:
            x = attention_layer(x)

        return torch.reshape(x, (-1, x.shape[1] * x.shape[2]))

def batchify_data(data):

    np.random.shuffle(data)

    # NOTE: I Was originally holding out some young players so as not to contaminate their predictions, but realized I can just include them in the validation set
    # pred_data = {player_lookup[d['_id']]:d['data'] for d in data if len(d['data'])<200}
    # data = [d for d in data if len(d['data'])>=200]

    split_size = int(len(data)*.85)
    train_data = {player_lookup[d["_id"]]: np.array(d['data']) for d in data[:split_size]}
    valid_data = {player_lookup[d["_id"]]: np.array(d['data']) for d in data[split_size:]}
    
    return train_data, valid_data


def train_loop(model, opt, loss_fn, dataloader):
    """
    Method from "A detailed guide to Pytorch's nn.Transformer() module.", by
    Daniel Melchor: https://medium.com/@danielmelchor/a-detailed-guide-to-pytorchs-nn-transformer-module-c80afbc9ffb1
    """
    
    model.train()
    total_loss = 0
    print(f"Stepping over {len(dataloader)} batches")
    for player, batch in dataloader.items():
        if len(batch)<100:
            continue
        print(f'[{player:<25}]\t loss: ', end='')
        
        X, y = batch, batch
        X, y = np.nan_to_num(X), np.nan_to_num(y)
        X, y = torch.tensor(X,dtype=torch.double).unsqueeze(0), torch.tensor(y,dtype=torch.double).unsqueeze(0)

        # Now we shift the tgt by one so with the <SOS> we predict the token at pos 1
        y_input = y[:,:-1]
        y_expected = y[:,1:]
        
        # Get mask to mask out the next words
        sequence_length = y_input.size(1)
        tgt_mask = model.get_tgt_mask(sequence_length).type(torch.double)

        # Standard training except we pass in y_input and tgt_mask
        pred = model(X, y_input, tgt_mask)

        # Permute pred to have batch size first again
        pred = pred.permute(1,0,2)
        loss = loss_fn(pred, y_expected)

        opt.zero_grad()
        loss.backward()
        opt.step()

        total_loss += loss.detach().item()

        print(loss.detach().item())
        
    return total_loss / len(dataloader)

def validation_loop(model, loss_fn, dataloader):
    """
    Method from "A detailed guide to Pytorch's nn.Transformer() module.", by
    Daniel Melchor: https://medium.com/@danielmelchor/a-detailed-guide-to-pytorchs-nn-transformer-module-c80afbc9ffb1
    """
    
    model.eval()
    total_loss = 0
    
    with torch.no_grad():
        for player, batch in dataloader.items():
            if len(batch)<100:
                continue

            print(f'[{player:<25}] loss: ', end='')
            
            X, y = batch, batch
            X, y = np.nan_to_num(X), np.nan_to_num(y)
            X, y = torch.tensor(X,dtype=torch.double).unsqueeze(0), torch.tensor(y,dtype=torch.double).unsqueeze(0)

            # Now we shift the tgt by one so with the <SOS> we predict the token at pos 1
            y_input = y[:,:-1]
            y_expected = y[:,1:]
            
            # Get mask to mask out the next words
            sequence_length = y_input.size(1)
            tgt_mask = model.get_tgt_mask(sequence_length).type(torch.double)

            # Standard training except we pass in y_input and src_mask
            pred = model(X, y_input, tgt_mask)
            pred = pred.permute(1,0,2)

            # Permute pred to have batch size first again  
            loss = loss_fn(pred, y_expected)
            total_loss += loss.detach().item()

            print(loss.detach().item())
        
    return total_loss / len(dataloader)



def predict_loop(model, input_sequence, length, xlsx_save_name):
    """
    Method from "A detailed guide to Pytorch's nn.Transformer() module.", by
    Daniel Melchor: https://medium.com/@danielmelchor/a-detailed-guide-to-pytorchs-nn-transformer-module-c80afbc9ffb1
    """
    model.eval()

    predictions = []

    with torch.no_grad():

        for player, sequence in input_sequence.items():

            sequence = torch.tensor(sequence, dtype=torch.double)
            y_input = sequence[:, -1:]

            X, y = sequence, sequence
            X, y = np.nan_to_num(X), np.nan_to_num(y)
            X, y = torch.tensor(X,dtype=torch.double).unsqueeze(0), torch.tensor(y,dtype=torch.double).unsqueeze(0)

            vector_fpoints = []

            for _ in range(length):

                # Now we shift the tgt by one so with the <SOS> we predict the token at pos 1
                y_input = y[:,:-1]
                
                # Get mask to mask out the next words
                sequence_length = y_input.size(1)
                tgt_mask = model.get_tgt_mask(sequence_length).type(torch.double)

                # Standard training except we pass in y_input and src_mask
                pred = model(X, y_input, tgt_mask)
                pred = pred.permute(1,0,2)

                fpoints = get_fantasy_points_from_game( encode_vector(pred[:,-1:][0][0].tolist()))

                y_input = torch.cat((y_input[:,1:], pred[:,-1:]), 1)

                vector_fpoints += [fpoints]

            avg = np.mean(vector_fpoints)

            print(f"{player:<25}: {avg}")

            predictions.append({player: {"FPoints": avg}})

    return predictions


def fit(model, opt, loss_fn, train_dataloader, val_dataloader, epochs):
    """
    Method from "A detailed guide to Pytorch's nn.Transformer() module.", by
    Daniel Melchor: https://medium.com/@danielmelchor/a-detailed-guide-to-pytorchs-nn-transformer-module-c80afbc9ffb1
    """
    
    # Used for plotting later on
    train_loss_list, validation_loss_list = [], []
    
    print("Training and validating model")
    for epoch in range(epochs):
        print("-"*25, f"Epoch {epoch + 1}","-"*25)
        
        train_loss = train_loop(model, opt, loss_fn, train_dataloader)
        train_loss_list += [train_loss]
        
        validation_loss = validation_loop(model, loss_fn, val_dataloader)
        validation_loss_list += [validation_loss]
        
        print(f"Training loss: {train_loss:.4f}")
        print(f"Validation loss: {validation_loss:.4f}")
        print()

        torch.save(model.state_dict(), f'v1-e{epoch}.state_dict')
        
    return train_loss_list, validation_loss_list


model = Transformer(input_shape=(1,35))
model = model.double()
# model.fit_scalar(np.concatenate([d['data'] for d in data]))
opt = torch.optim.SGD(model.parameters(), lr=0.1)
loss_fn = nn.MSELoss()

device = "cuda" if torch.cuda.is_available() else "cpu"

train_loss_list, validation_loss_list = fit(model, opt, loss_fn, train_data, valid_data, 10)



print("Train loss list: ", end='')
print(train_loss_list)

print("Validation loss list: ", end='')
print(validation_loss_list)

predict_loop(model, valid_data, 30, "validation_predictions.xlsx")
predict_loop(model, {**valid_data, **train_data}, 30, "all_players.xlsx")
