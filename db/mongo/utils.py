import time

from .enums import DATABASES, NBA_COLLECTIONS

from pymongo import MongoClient
from pymongo.database import Database

from nba_api.stats.static.players import get_players, get_active_players
from nba_api.stats.static.teams import get_teams
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo

client = MongoClient('localhost', 27017)

def update_mongo_players():
    '''
    Update mongo db Players collection. Returns added players, which can be used to update other related databases.

    Returns: List of the player ids that were added to the player database
    '''

    collection = client[DATABASES.NBA][NBA_COLLECTIONS.Players] # relevant collection to be updated
    players = get_players() # get's a list of dictionaries, each representing a player
    players_added = []

    # for each player in NBA history, check if they're in the db. If not, add them
    for player in players:
        if collection.find_one({'id': player['id']}) == None:
            print(f"[mongo.utils] Adding {player['full_name']} to {DATABASES.NBA}.{NBA_COLLECTIONS.Players}")
            collection.insert_one(player)
            players_added.append(player['id'])
    
    return players_added


def update_mongo_cpi(player_ids:list):
    '''
    Update mongo db with common player info (cpi)
    '''
    collection = client[DATABASES.NBA][NBA_COLLECTIONS.CommonPlayerInfo]
    collection_players = client[DATABASES.NBA][NBA_COLLECTIONS.Players]
    players_added = []
    
    for player_id in player_ids:
        if collection.find_one(player_id) == None:

            player = collection_players.find_one({"id": player_id})
            print(f"[mongo.utils] Adding {player['full_name']} to {DATABASES.NBA}.{NBA_COLLECTIONS.CommonPlayerInfo}")
            cpi = CommonPlayerInfo(player['id']).get_normalized_dict()['CommonPlayerInfo'][0]
            collection.insert_one(cpi)
            players_added.append(player['id'])
            time.sleep(2)
