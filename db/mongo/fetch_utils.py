import time
from datetime import datetime
from calendar import monthrange
from tqdm import tqdm
from nba_api.stats.static.players import get_players
from pymongo import MongoClient
from pymongo.database import Database
from .enums import DATABASES, NBA_COLLECTIONS
from nba_api.stats.library.parameters import GameDate
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo
from nba_api.stats.endpoints.boxscoretraditionalv2 import BoxScoreTraditionalV2
from nba_api.stats.endpoints.scoreboardv2 import ScoreboardV2

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


def get_all_game_ids_since_date(year, month, day, check_and_update_mongo_players=True):
    '''
    Give this function a year, month, and day, and it will retreive all of the game ids that have occured between then and up 
    until (but not including) today.
    Args:
        year (int)                                  Earliest year to collect game ids from
        month (int)                                 Earliest month to collect game ids from
        day (int)                                   Earliest day to collect game ids from
        check_and_update_mongo_players (bool)    If True, after collecting all the game ids, it will use each one to fetch box scores
                                                    using the box scores endpoint. Any player ids it finds, it will create an entry in the 
                                                    NBA.Players collection, AND UPDATE NBA.CommonPlayerInfo. This can be incredibly useful 
                                                    for rookies, which for some reason don't get found by nba_api's get_players() built-in method.
    '''
    today = datetime.today()
    now_y, now_m, now_d = today.year, today.month, today.day
    game_ids = []
    print(f"Collecting all game ids since {year}-{month}-{day}")
    for y in range(year, now_y+1):
        for m in range(month, now_m+1):
            first_day = day if ((y==year) and (m==month)) else 1
            last_day = now_d if ((y==now_y) and (m==now_m)) else monthrange(y, m)[1]
            for d in range(first_day, last_day): # Don't want to get today's games
                time.sleep(2)
                scoreboard = ScoreboardV2(game_date=GameDate().get_date(y, m, d)).get_normalized_dict()
                for sb in scoreboard['GameHeader']:
                    if sb['GAME_ID'][:3] == '002': # '002' means regular season
                        game_ids.append(sb['GAME_ID'])
                print(f"Finished {y}-{m}-{d}.")

    if check_and_update_mongo_players:
        print("Using the discovered game ids to update mongo NBA.Players collection for missing players. NOTE: This also updates NBA.CommonPlayerInfo, BUT NOT ANYWHERE ELSE")
        update_mongo_players_from_game_ids(game_ids)

    return game_ids

def update_mongo_players_from_game_ids(game_ids):
    '''
    Hand this a list of game ids and it will find the box scores, and populate mongo NBA.Players and NBA.CommonPlayerInfo with the appropriate
    information
    '''
    missing_players_ids, nonmissing_player_ids = set(), set() # nonmissing_player_ids for caching to decrease mongo communication
    for gid in tqdm(game_ids, desc=f'Checking {len(game_ids)} games and updating...'):
        list_of_players_in_game = BoxScoreTraditionalV2(gid).get_normalized_dict()['PlayerStats']
        time.sleep(2)
        for player in list_of_players_in_game:
            pid = player['PLAYER_ID']
            if pid in nonmissing_player_ids or pid in missing_players_ids: # then we've already done this player
                continue
            else:
                players = [p for p in client.NBA.Players.find({"PLAYER_ID": pid})]
                if len(players)==0:
                    print(f"{player['PLAYER_NAME']} was not in NBA.Players. Updating here, and also in CommonPlayerInfo")
                    cpi = CommonPlayerInfo(pid).get_normalized_dict()['CommonPlayerInfo'][0]
                    time.sleep(2)
                    
                    # NBA.Players Entry
                    insert = {
                        'PLAYER_ID': pid,
                        'PLAYER_NAME': cpi['DISPLAY_FIRST_LAST'],
                        'IS_ACTIVE': True if cpi['ROSTERSTATUS']=='Active' else False,
                        'LAST_NAME': cpi['LAST_NAME'],
                        'FIRST_NAME': cpi['FIRST_NAME']
                    }
                    client.NBA.Players.insert_one(insert)

                    # NBA.CommonPlayerInfo Entry
                    cpi['PLAYER_ID'] = pid
                    del cpi['PERSON_ID']
                    client.NBA.CommonPlayerInfo.replace_one({"PLAYER_ID": pid}, cpi, upsert=True)
                    missing_players_ids.add(pid)

                elif len(players)==1:
                    nonmissing_player_ids.add(pid)

                else:
                    print("Error. PID should not match more than one entry in NBA.Players")
                    input()
                    exit()
    return missing_players_ids, nonmissing_player_ids