import sys, os
from datetime import datetime
from calendar import monthrange
from pymongo import MongoClient
from pymongo.message import update
sys.path.append(r"/home/jakemdaly/Documents/GitRepos/nba-ml")

from dfs.dfs_utils import get_draft_kings_available_players, update_dk_player_game_infos, update_dkplayergameinfosteams_with_abb
from db.mongo.populate_utils import update_mongo_player_game_logs_adv_by_game_id, update_mongo_player_game_logs_by_game_id
from db.mongo.fetch_utils import get_all_game_ids_since_date

now = datetime.now()

if now.day == 1:
    if now.month == 1:
        year = now.year-1
        month = 12
        day = monthrange(year, month)[1]
    else:
        year = now.year
        month = now.month - 1
        day = monthrange(year, month)[1]
else:
    year = now.year
    month = now.month
    day = now.day-1

ids = get_all_game_ids_since_date(year, month, day, True)
update_mongo_player_game_logs_by_game_id(ids)
update_mongo_player_game_logs_adv_by_game_id(ids)

# Do Dk update after nba, because nba might add players to Players db that dk function uses
dk_players = get_draft_kings_available_players()
update_dk_player_game_infos(dk_players)
update_dkplayergameinfosteams_with_abb()

