import sys, os
sys.path.append(r"/home/jakemdaly/Documents/GitRepos/nba-ml")
from nba.utils import get_fantasy_points_from_game, FRules_DK_Classic
import pymongo
import numpy as np
from datetime import datetime
from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd
from scipy.stats import zscore
from copy import deepcopy
import pdb
from torch import nn
from abc import ABC, abstractclassmethod

client = MongoClient('localhost', 27017).NBA
class MissingPlayerDocument(Exception):
    pass

YEAR = 2021
MONTH = 12
TODAY = 21



def run_model_evaluation():
    '''For all models, this function will evaluate how we did last night, and store that in an evaluation collection inside mongo'''

    # Fetch all of the models / heuristics

    pass

class model_base(ABC):

    def __init__(self):
        pass
    @abstractclassmethod
    def fit(self):
        pass
    @abstractclassmethod
    def predict(self):
        pass

class heuristicCoarseGrainedScore(model_base):
    pass

class heuristicFineGrainedScore(model_base):
    pass

class heuristicNormalizedScore(model_base):
    pass

class modelRandomForrest(model_base):
    pass

class modelNN(nn.Module, model_base):
    pass




def get_master_dataframe():
    '''Returns a dataframe whos data is a combination of a player game log and a DK game info'''

    data = []
    bad_names = set()
    try:
        all_dk_docs = client.DKPlayerGameInfos.find({})
        for dk_doc in all_dk_docs:
            game_date = dk_doc['GAME_DATE']
            player    = dk_doc['PLAYER_NAME']

            player_game_log = client.PlayerGameLogs.find({"PLAYER_NAME": player, "GAME_DATE": game_date})
            if not player_game_log:
                bad_names.add(player)
                continue
            if len(player_game_log) != 1:
                # this means something went wrong... fix
                continue
            data.append({**dk_doc, **player_game_log[0]})

    except:
        pass

    df = pd.DataFrame(data)

    return df

def dk_dataset_updater(df):
    """This function will extract features from the master dataframe returned by get_master_dataframe and build a supervised learning. As we want to build different models/datasets
    and try different features, we should start by adding them to this. This also returns a dataframe """