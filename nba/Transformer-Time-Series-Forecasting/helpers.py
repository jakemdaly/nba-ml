import os, shutil
from pymongo import MongoClient
import numpy as np

# save train or validation loss
def log_loss(loss_val : float, path_to_save_loss : str, train : bool = True):
    if train:
        file_name = "train_loss.txt"
    else:
        file_name = "val_loss.txt"

    path_to_file = path_to_save_loss+file_name
    os.makedirs(os.path.dirname(path_to_file), exist_ok=True)
    with open(path_to_file, "a") as f:
        f.write(str(loss_val)+"\n")
        f.close()

# Exponential Moving Average, https://en.wikipedia.org/wiki/Moving_average
def EMA(values, alpha=0.1):
    ema_values = [values[0]]
    for idx, item in enumerate(values[1:]):
        ema_values.append(alpha*item + (1-alpha)*ema_values[idx])
    return ema_values

# Remove all files from previous executions and re-run the model.
def clean_directory():

    if os.path.exists('save_loss'):
        shutil.rmtree('save_loss')
    if os.path.exists('save_model'): 
        shutil.rmtree('save_model')
    if os.path.exists('save_predictions'): 
        shutil.rmtree('save_predictions')
    os.mkdir("save_loss")
    os.mkdir("save_model")
    os.mkdir("save_predictions")

def encode_vector(vector):
    categories = ['MIN', 'OFF_RATING', 'DEF_RATING', 'NET_RATING', 'AST_PCT', 'OREB_PCT', 'DREB_PCT', 'EFG_PCT', 'TS_PCT', 'USG_PCT', 'PACE', 'POSS', 'PIE', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'PLUS_MINUS', 'PLAYER_AGE', 'START_F', 'START_C', 'START_G', 'FANTASY_POINTS_v0', 'DAYS_SINCE_LAST_GAME']
    encoding = dict()
    for vec_el, category in zip(vector, categories):
        encoding[category] = vec_el

    return encoding

client = MongoClient('localhost', 27017)
nba_db = client['NBA']
dataset = nba_db.DatasetPlayerGameLogs
ids = dataset.distinct("PLAYER_ID")