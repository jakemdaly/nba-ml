import sys, os
import time
import pickle
from datetime import datetime
from calendar import monthrange
from tqdm import tqdm
from pymongo import MongoClient
sys.path.append(r"/home/jakemdaly/Documents/GitRepos/nba-ml")

from db.mongo.populate_utils import update_mongo_player_game_logs_adv_by_game_id, update_mongo_player_game_logs_by_player_id, update_mongo_player_game_logs_by_game_id
from db.mongo.fetch_utils import get_all_game_ids_since_date, update_mongo_players_from_game_ids
from dfs.dfs_utils import update_dk_player_game_infos_from_csv, update_dkplayergameinfosteams_with_abb, update_latest_injury_report_mongo

from nba_api.stats.library.parameters import GameDate
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo
from nba_api.stats.endpoints.boxscoretraditionalv2 import BoxScoreTraditionalV2
from nba_api.stats.endpoints.scoreboardv2 import ScoreboardV2

update_dkplayergameinfosteams_with_abb()