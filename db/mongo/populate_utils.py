import time
import copy

from nba_api.stats.library.parameters import MeasureTypeDetailed
from nba_api.stats.library.parameters import SeasonAll

from .enums import DATABASES, NBA_COLLECTIONS

from pymongo import MongoClient
from pymongo.database import Database

from nba_api.stats.static.players import get_players, get_active_players
from nba_api.stats.static.teams import get_teams
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo
from nba_api.stats.endpoints.playergamelog import PlayerGameLog

client = MongoClient('localhost', 27017)

def update_mongo_players():
    '''
    Update mongo db Players collection. Returns added players, which can be used to update other related databases.

    Returns: 
        remaining_ids (list):   List of the player ids that were not successfully added to the collection
    '''

    collection = client[DATABASES.NBA][NBA_COLLECTIONS.Players] # relevant collection to be updated
    players = get_players() # get's a list of dictionaries, each representing a player
    remaining_ids = [player['id'] for player in players]

    # for each player in NBA history, check if they're in the db. If not, add them
    for player in players:
        if collection.find_one({'id': player['id']}) == None:
            print(f"[mongo.utils] Adding {player['full_name']} to {DATABASES.NBA}.{NBA_COLLECTIONS.Players}")
            collection.insert_one(player)
            remaining_ids.remove(player['id'])
    
    return remaining_ids


def update_mongo_cpi(player_ids:list):
    '''
    Update mongo db with common player info (cpi). Assumes that NBA.Players is fully up to date. Note that every 
    season, all players should be updated because there are fields in CommonPlayerInfo that might need updating 
    (eg. roster status = active -> inactive). This function might fail at some point (for one reason or another). 
    It will return a list of the ids which it wasn't able to add to the collection. 

    Args:
        player_ids (list):      list of player ids (which we are trying to update)

    Returns:
        remaining_ids (list):   list of ids which did not successfully get added to the collection
    '''
    collection = client[DATABASES.NBA][NBA_COLLECTIONS.CommonPlayerInfo]
    collection_players = client[DATABASES.NBA][NBA_COLLECTIONS.Players]
    remaining_ids = copy.deepcopy(player_ids)

    # for each player id in the specified player_ids list...
    for player_id in player_ids:

        # get this player's info from our NBA.Player collection
        player = collection_players.find_one({"id": player_id})

        try:
            # get the common player info from NBA.com
            cpi = CommonPlayerInfo(player['id']).get_normalized_dict()['CommonPlayerInfo'][0]
            # standardize id field
            cpi['id'] = player['id']
            del cpi['PERSON_ID']


            # ... if it already exists we're going to delete it first (ie perform an update)
            if collection.find_one(player_id) != None:
                collection.delete_one({"id": player_id})

            # ... then either way update with the new entry
            collection.insert_one(cpi)

            # if successful we can remove this from the remaining ids to do
            remaining_ids.remove(player['id'])
            time.sleep(2)

            print(f"[db.mongo.utils.update_mongo_cpi] Successfully udpated {player['full_name']} in NBA.CommonPlayerInfo")

        except:
            continue

    return remaining_ids


def update_mongo_player_game_logs(player_ids:list):
    '''
    This calls PlayerGameLog, which actually returns all of the game logs for a given player. The game logs have basic
    stats for the player, and more importantly, the game IDS for a player, which can be used to get other information
    like advanced box score for that player for that game.
    Args:
        player_ids (list):      list of player ids (which we are trying to update)

    Returns:
        remaining_ids (list):   list of ids which did not successfully get added to the collection
    '''
    collection = client[DATABASES.NBA][NBA_COLLECTIONS.PlayerGameLogs]
    collection_players = client[DATABASES.NBA][NBA_COLLECTIONS.Players]
    remaining_ids = copy.deepcopy(player_ids)

    # for each player id in the specified player_ids list...
    for player_id in player_ids:

        # get this player's info from our NBA.Player collection
        player = collection_players.find_one({"id": player_id})

        try:
            # get the common player info from NBA.com
            df_player_game_logs = PlayerGameLog(player['id'], season=SeasonAll.all).get_data_frames()[0]

            # need to convert indices to strings so that we can store in pymongo collection
            df_player_game_logs.index = df_player_game_logs.index.map(str)
            for k, row in df_player_game_logs.iterrows():
                insert = dict(row)
                insert.update(
                    {'full_name': player['full_name'],
                    'id': player['id']}
                )
                insert.pop('Player_ID')
                query = {'$and': [{'full_name': insert['full_name']}, {'Game_ID': insert['Game_ID']}]}
                already_in_collection = collection.find_one(query)
                
                # ... if it already exists we're going to delete it first (ie perform an update)
                if already_in_collection:
                    collection.delete_one(query)
                
                # ... then either way update with the new entry
                collection.insert_one(query)

            # if successful we can remove this from the remaining ids to do
            remaining_ids.remove(player['id'])
            time.sleep(2)

            print(f"[db.mongo.utils.update_mongo_player_game_logs] Successfully udpated {player['full_name']} in NBA.PlayerGameLogs")

        except:
            print(f"\n[db.mongo.utils.update_mongo_player_game_logs] COULDN'T ADD {player['full_name']} to NBA.PlayerGameLogs\n")
            continue

    return remaining_ids