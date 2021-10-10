from nba_api.stats.static.players import get_players
from pymongo import MongoClient
from pymongo.database import Database
from .enums import DATABASES, NBA_COLLECTIONS

client = MongoClient('localhost', 27017)

def players_db_needs_updating()->bool:
    '''Use this to check if any players are missing from the Players database. This is critical because the players db is used by other functions'''
    db_all_players = get_all_player_ids()
    api_all_players = [player['id'] for player in get_players()]
    return set(db_all_players) != set(api_all_players)

def get_all_player_ids():
    '''Returns a list of all the player ids from Mongo'''
    return client[DATABASES.NBA][NBA_COLLECTIONS.Players].distinct("PLAYER_ID")

def get_all_game_ids():
    '''Returns a list of all game IDs contained in the PlayerGameLogs collection'''
    return client[DATABASES.NBA][NBA_COLLECTIONS.PlayerGameLogs].distinct("GAME_ID")

def get_all_players():
    '''Returns the information from the Players collection'''
    return client[DATABASES.NBA][NBA_COLLECTIONS.Players].find()

def get_all_team_ids():
    '''Returns a list of all the player ids from Mongo'''
    return client[DATABASES.NBA][NBA_COLLECTIONS.Teams].distinct("TEAM_ID")

def get_all_teams():
    return client[DATABASES.NBA][NBA_COLLECTIONS.Teams].find()