import sys, os
from datetime import datetime
from calendar import monthrange
from pymongo import MongoClient
from pymongo.message import update

sys.path.append(r"/home/jakemdaly/Documents/GitRepos/nba-ml")

from dfs.models import run_model_evaluation
from db.mongo.fetch_utils import get_all_game_ids_since_date
from db.mongo.utils import verify_game_logs, verify_game_logs_adv, get_year_month_day_yesterday

client = MongoClient('localhost', 27017).NBA
clientDS = MongoClient('localhost', 27017).Datasets
# The flow will generally be:

def main():

    # get yesterday's year, month, and day
    year, month, day = get_year_month_day_yesterday()

    # Make sure Mongo has the latest DK data and player game logs
    ids = get_all_game_ids_since_date(year, month, day, False)
    # Do some checking to see if yesterdays stats are actually in there
    verify_game_logs(ids)
    verify_game_logs_adv(ids)    

    # Update the DK data set
    yesterday_dk_game_infos = client.DKPlayerGameInfos.find({"GAME_DATE":datetime(year, month, day)})
    for dk_game_info in yesterday_dk_game_infos:
        game_log = handle_game_log(client.PlayerGameLogs.find({"GAME_DATE":datetime(year, month, day), "PLAYER_NAME": dk_game_info['PLAYER_NAME']}))
        combined = {**dk_game_info, **game_log}
        if len(clientDS.DKDataset.distinct("_id", {"PLAYER_NAME": combined['PLAYER_NAME'], "GAME_DATE": combined["GAME_DATE"]})) == 0:
            clientDS.DKDataset.insert_one(combined)

    # Run evaluation for last nights models / heuristics
    # last_nights_data = clientDS.DKDataset.find({"GAME_DATE": datetime(year, month, day)})
    run_model_evaluation()

    # Train the model(s) with the latest data (full or incremental)

    # Do inferences on tonights players with all models

    # Store these 

def handle_game_log(game_log_mongo_doc):
    """When we search Mongo for a PLAYER_NAME, GAME_DATE combo, we want it to match exactly one doc. This verifies / handles that"""
    if game_log_mongo_doc is None:
        return # player had a dk doc, but no game log. This very well could have happened
    if len(game_log_mongo_doc) == 1:
        return game_log_mongo_doc[0]
    else:
        print("Found more than one game log for this player and gamedate")
        return


if __name__=="__main__":
    main()