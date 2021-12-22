import sys, os
from datetime import datetime
from calendar import monthrange
from pymongo import MongoClient
from pymongo.message import update
sys.path.append(r"/home/jakemdaly/Documents/GitRepos/nba-ml")

from dfs.dfs_utils import get_draft_kings_available_players, update_dk_player_game_infos, update_dkplayergameinfosteams_with_abb
from dfs.models import run_model_evaluation
from db.mongo.populate_utils import update_mongo_player_game_logs_adv_by_game_id, update_mongo_player_game_logs_by_game_id
from db.mongo.fetch_utils import get_all_game_ids_since_date
from db.mongo.utils import verify_game_logs, verify_game_logs_adv, get_year_month_day_yesterday

year, month, day = get_year_month_day_yesterday()

###
## DATA POPULATION
# The below two sections populate mongo with db from the various apis

# Do nba_api (aka statistics) updates
ids = get_all_game_ids_since_date(year, month, day, True)
update_mongo_player_game_logs_by_game_id(ids)
update_mongo_player_game_logs_adv_by_game_id(ids)
 
# Do Dk update after nba, because nba might add players to Players db that dk function uses
dk_players = get_draft_kings_available_players()
update_dk_player_game_infos(dk_players)
update_dkplayergameinfosteams_with_abb()


###
## DATA VERIFICATION
# The below sections check to see which data actually got added into mongo

# Do some checking to see if yesterdays stats are actually in there
verify_game_logs(ids)
verify_game_logs_adv(ids)


###
## Measuring model performance
# Below is where we have several models/heuristics created for choosing optimal lineups. Each night we record their performance into a collection
# run_model_evaluation()