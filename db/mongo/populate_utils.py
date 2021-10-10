import time
import copy


from .enums import DATABASES, NBA_COLLECTIONS
from .utils import get_season_str_YY, get_current_year
from datetime import datetime
from pymongo import MongoClient
from pymongo.database import Database
from tqdm import tqdm


from nba_api.stats.library.parameters import MeasureTypeDetailed, PerModeDetailed, SeasonAll
from nba_api.stats.static.players import get_players, get_active_players
from nba_api.stats.static.teams import get_teams
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo
from nba_api.stats.endpoints.playergamelog import PlayerGameLog
from nba_api.stats.endpoints.boxscoreadvancedv2 import BoxScoreAdvancedV2
from nba_api.stats.endpoints.playerdashboardbylastngames import PlayerDashboardByLastNGames
from nba_api.stats.endpoints.teamdashboardbygeneralsplits import TeamDashboardByGeneralSplits

client = MongoClient('localhost', 27017)

def update_mongo_players():
    '''
    Update mongo db Players collection. Returns players which were not successfully added for whatever reason

    Returns: 
        remaining_ids (list):   List of the player ids that were not successfully added to the collection
    '''

    collection = client[DATABASES.NBA][NBA_COLLECTIONS.Players] # relevant collection to be updated
    players = get_players() # get's a list of dictionaries, each representing a player
    remaining_ids = [player['id'] for player in players]

    # for each player in NBA history, check if they're in the db. If not, add them
    for player in players:
        insert = {
            'PLAYER_ID': player['id'],
            'PLAYER_NAME': player['full_name'],
            'IS_ACTIVE': player['is_active'],
            'LAST_NAME': player['last_name'],
            'FIRST_NAME': player['first_name']
        }

        # if player doesn't exist, add them
        if collection.find_one({'PLAYER_ID': insert['PLAYER_ID']}) == None:
            print(f"[mongo.utils] Adding {insert['PLAYER_NAME']} to {NBA_COLLECTIONS.Players}")
            collection.insert_one(insert)
        # if player does exist, remove them and then re-add for an update
        else:
            print(f"[mongo.utils] Updating {insert['PLAYER_NAME']} in {NBA_COLLECTIONS.Players}")
            collection.delete_one({"PLAYER_ID": insert['PLAYER_ID']})
            collection.insert_one(insert)

        remaining_ids.remove(player['id'])
    return remaining_ids

def update_mongo_teams():
    '''
    Update mongo db Teams collection. Returns list of team ids which could not be successfully added to collection

    Returns: 
        remaining_ids (list):   List of the team ids that were not successfully added to the collection
    '''

    collection = client[DATABASES.NBA][NBA_COLLECTIONS.Teams] # relevant collection to be updated
    teams = get_teams() # get's a list of dictionaries, each representing a player
    remaining_ids = [team['id'] for team in teams]

    # for each team in NBA history, check if they're in the db. If not, add them
    for team in teams:
        insert = {'TEAM_ID': team['id'],
        'TEAM_NAME': team['full_name'],
        'ABBREVIATION': team['abbreviation'],
        'NICKNAME': team['nickname'],
        'CITY': team['city'],
        'STATE': team['state'],
        'YEAR_FOUNDED': team['year_founded']}

        # if team doesn't exist, add them
        if collection.find_one({'TEAM_ID': insert['TEAM_ID']}) == None:
            print(f"[mongo.utils] Adding {insert['TEAM_NAME']} to {NBA_COLLECTIONS.Teams}")
            collection.insert_one(insert)
        
        # if player does exist, remove them and then re-add for an update
        else:
            print(f"[mongo.utils] Updating {insert['TEAM_NAME']} in {NBA_COLLECTIONS.Teams}")
            collection.delete_one({"TEAM_NAME": insert['TEAM_NAME']})
            collection.insert_one(insert)

        remaining_ids.remove(team['id'])

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
        player = collection_players.find_one({"PLAYER_ID": player_id})

        try:
            # get the common player info from NBA.com
            cpi = CommonPlayerInfo(player['PLAYER_ID']).get_normalized_dict()['CommonPlayerInfo'][0]
            # standardize id field
            cpi['PLAYER_ID'] = player['PLAYER_ID']
            del cpi['PERSON_ID']

            # ... then either way update with the new entry
            collection.replace_one({"PLAYER_ID": player_id}, cpi, upsert=True)

            # if successful we can remove this from the remaining ids to do
            remaining_ids.remove(player['PLAYER_ID'])
            time.sleep(2)

            print(f"[db.mongo.utils.update_mongo_cpi] Successfully udpated {player['PLAYER_NAME']} in {NBA_COLLECTIONS.CommonPlayerInfo}")

        except:
            print(f"[db.mongo.utils.update_mongo_cpi] COULDN'T UPDATE {player['PLAYER_NAME']} IN {NBA_COLLECTIONS.CommonPlayerInfo}")
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
        player = collection_players.find_one({"PLAYER_ID": player_id})

        try:
            # get the common player info from NBA.com
            df_player_game_logs = PlayerGameLog(player['PLAYER_ID'], season=SeasonAll.all).get_data_frames()[0]

            # need to convert indices to strings so that we can store in pymongo collection
            df_player_game_logs.index = df_player_game_logs.index.map(str)
            for k, row in df_player_game_logs.iterrows():
                insert = dict(row)

                # standardize names of columns
                insert.update(
                    {'PLAYER_NAME': player['PLAYER_NAME'],
                    'PLAYER_ID': player['PLAYER_ID'],
                    'GAME_ID': insert['Game_ID']}
                )
                insert.pop('Player_ID')
                insert.pop('Game_ID')
                try:
                    insert['GAME_DATE'] = datetime.strptime(insert['GAME_DATE'], "%b %d, %Y")
                except ValueError:
                    raise ValueError(f"Incorrect data format.")

                #check if already in collection
                query = {'$and': [{'PLAYER_ID': insert['PLAYER_ID']}, {'GAME_ID': insert['GAME_ID']}]}
                already_in_collection = collection.find_one(query)
                
                # ... if it already exists we're going to delete it first (ie perform an update)
                if already_in_collection:
                    collection.delete_one(query)
                
                # ... then either way update with the new entry
                collection.insert_one(insert)

            # if successful we can remove this from the remaining ids to do
            remaining_ids.remove(player['PLAYER_ID'])
            time.sleep(2)

            print(f"[db.mongo.utils.update_mongo_player_game_logs] Successfully udpated {player['PLAYER_NAME']} in {NBA_COLLECTIONS.PlayerGameLogs}")

        except:
            print(f"\n[db.mongo.utils.update_mongo_player_game_logs] COULDN'T ADD {player['PLAYER_NAME']} to {NBA_COLLECTIONS.PlayerGameLogs}\n")
            continue

    return remaining_ids

def update_mongo_player_game_logs_adv_by_game_id(game_ids:list):

    collection = client[DATABASES.NBA][NBA_COLLECTIONS.PlayerGameLogsAdv]
    remaining_ids = copy.deepcopy(game_ids)

    # for each player id in the specified player_ids list...
    for game_id in game_ids:

        try:
            # get the common player info from NBA.com
            df = BoxScoreAdvancedV2(game_id).get_data_frames()[0]

            # need to convert indices to strings so that we can store in pymongo collection
            df.index = df.index.map(str)
            for k, row in df.iterrows():
                insert = dict(row)
                query = {'$and': [{'PLAYER_NAME': insert['PLAYER_NAME']}, {'GAME_ID': insert['GAME_ID']}]}
                already_in_collection = collection.find_one(query)
                print("THIS MESSAGE SERVES AS A BREAKPOINT. SEE IF YOU CAN EASILY ADD IN GAME_DATE AT THIS POINT IN THE CODE. RESTART THIS FUNCTION AND PUT A REAL BREAKPOINT.")
                input()
                
                # ... if it already exists we're going to delete it first (ie perform an update)
                if already_in_collection:
                    collection.delete_one(query)

                # ... then either way update with the new entry
                collection.insert_one(insert)

            # if successful we can remove this from the remaining ids to do
            remaining_ids.remove(game_id)
            time.sleep(2)

            print(f"[db.mongo.utils.update_mongo_player_game_logs] Successfully udpated game {game_id} in {NBA_COLLECTIONS.PlayerGameLogsAdv}")

        except:
            print(f"\n[db.mongo.utils.update_mongo_player_game_logs] COULDN'T ADD GAME {game_id} to {NBA_COLLECTIONS.PlayerGameLogsAdv}\n")
            continue

    return remaining_ids


def update_mongo_player_season_stats(player_ids:list):

    # Get some of the collections we will be using
    collection = client[DATABASES.NBA][NBA_COLLECTIONS.PlayerSeasonStats]
    collection_cpi = client[DATABASES.NBA][NBA_COLLECTIONS.CommonPlayerInfo]
    collection_players = client[DATABASES.NBA][NBA_COLLECTIONS.Players]
    remaining_ids = copy.deepcopy(player_ids)

    # for each player id in the specified player_ids list...
    for player_id in tqdm(player_ids, desc='Getting basic season stats for all players.', position=0, leave=True):
        
        player = collection_players.find_one({'PLAYER_ID': player_id})
        # get this player's info from our NBA.Player collection
        cpi = collection_cpi.find_one({"PLAYER_ID": player_id})
        if not cpi:
            print(f"\n\n\nNo CPI for id {player_id}\n\n\n")
            break
        
        bad_season_detected = False # if we detect a bad season, this will get set to true, and the id won't be removed from remaining ids
        print(f"Getting {player['PLAYER_NAME']}'s", end="")
        for year in range(int(cpi['FROM_YEAR']), int(cpi['TO_YEAR'])+1):
            
            try:
                # set up query year
                season_first_half = year
                season_second_half = get_season_str_YY(year+1)
                season_str = '{}-{}'.format(season_first_half, season_second_half)
                
                # query to check if this is already in mongo
                query = {"$and": [{"PLAYER_ID": player['PLAYER_ID']}, {'SEASON': season_str}]}
                already_in_collection = collection.find_one(query)

                # if it's not already there, we will try to add it
                if not already_in_collection:
                    print(f"...{season_str} season ", end="")
                    dash_basic = PlayerDashboardByLastNGames(player_id=player['PLAYER_ID'], season=season_str, measure_type_detailed=MeasureTypeDetailed.base, per_mode_detailed=PerModeDetailed.per_game).get_data_frames()[0] # 0 is overall (season long)
                    time.sleep(2)
                    if len(dash_basic):
                        print(f"\n Updating with {season_str} data")
                        for k, row in dash_basic.iterrows():
                            insert = dict(row)
                            keys = list(insert.keys())
                            # There are some junk columns we don't want to store
                            for key in keys:
                                if 'RANK' in key or 'CF' in key:
                                    insert.pop(key)
                            # Add some useful information
                            insert['PLAYER_ID'] = player['PLAYER_ID']
                            insert['SEASON'] = season_str
                            insert['PLAYER_NAME'] = player['PLAYER_NAME']
                            del insert['GROUP_VALUE']
                            del insert['GROUP_SET']
                            collection.insert_one(insert)
                    else:
                        collection.insert_one({
                            "PLAYER_ID": player['PLAYER_ID'],
                            "SEASON": season_str,
                            "PLAYER_NAME": player['PLAYER_NAME']
                        })
            
            except:
                print(f"\n\n\n[db.mongo.utils.update_mongo_player_season_stats] COULDN'T ADD {player['PLAYER_NAME']}'s {season_str} season to {NBA_COLLECTIONS.PlayerSeasonStats}\n\n\n")
                bad_season_detected = True
                continue

        # if there was a bad season detected, we want to leave this in remaining IDs for post-processing
        if not bad_season_detected:
            remaining_ids.remove(player_id)

        print(f"[db.mongo.utils.update_mongo_player_season_stats] Successfully udpated {player['PLAYER_NAME']} in {NBA_COLLECTIONS.PlayerSeasonStats}")

    return remaining_ids


def update_mongo_player_season_stats_adv(player_ids:list):

    # Get some of the collections we will be using
    collection = client[DATABASES.NBA][NBA_COLLECTIONS.PlayerSeasonStatsAdv]
    collection_cpi = client[DATABASES.NBA][NBA_COLLECTIONS.CommonPlayerInfo]
    collection_players = client[DATABASES.NBA][NBA_COLLECTIONS.Players]
    remaining_ids = copy.deepcopy(player_ids)

    # for each player id in the specified player_ids list...
    for player_id in tqdm(player_ids, desc='Getting advanced season stats for all players.', position=0, leave=True):
        
        player = collection_players.find_one({'PLAYER_ID': player_id})
        # get this player's info from our NBA.Player collection
        cpi = collection_cpi.find_one({"PLAYER_ID": player_id})
        if not cpi:
            print(f"\n\n\nNo CPI for id {player_id}\n\n\n")
            break
        
        bad_season_detected = False # if we detect a bad season, this will get set to true, and the id won't be removed from remaining ids
        print(f"Getting {player['PLAYER_NAME']}'s", end="")
        for year in range(int(cpi['FROM_YEAR']), int(cpi['TO_YEAR'])+1):
            
            try:
                # set up query year
                season_first_half = year
                season_second_half = get_season_str_YY(year+1)
                season_str = '{}-{}'.format(season_first_half, season_second_half)
                
                # query to check if this is already in mongo
                query = {"$and": [{"PLAYER_ID": player['PLAYER_ID']}, {'SEASON': season_str}]}
                already_in_collection = collection.find_one(query)

                # if it's not already there, we will try to add it
                if not already_in_collection:
                    print(f"...{season_str} season ", end="")
                    dash_adv = PlayerDashboardByLastNGames(player_id=player['PLAYER_ID'], season=season_str, measure_type_detailed=MeasureTypeDetailed.advanced, per_mode_detailed=PerModeDetailed.per_game).get_data_frames()[0] # 0 is overall (season long)
                    time.sleep(2)
                    if len(dash_adv):
                        print(f"\n Updating with {season_str} data")
                        for k, row in dash_adv.iterrows():
                            insert = dict(row)
                            keys = list(insert.keys())
                            # There are some junk columns we don't want to store
                            for key in keys:
                                if 'RANK' in key or 'CF' in key or 'sp_work' in key:
                                    insert.pop(key)
                            # Add some useful information
                            insert['PLAYER_ID'] = player['PLAYER_ID']
                            insert['SEASON'] = season_str
                            insert['PLAYER_NAME'] = player['PLAYER_NAME']
                            del insert['GROUP_VALUE']
                            del insert['GROUP_SET']
                            collection.insert_one(insert)
                    else:
                        collection.insert_one({
                            "PLAYER_ID": player['PLAYER_ID'],
                            "SEASON": season_str,
                            "PLAYER_NAME": player['PLAYER_NAME']
                        })
            
            except:
                print(f"\n\n\n[db.mongo.utils.update_mongo_player_season_stats_adv] COULDN'T ADD {player['PLAYER_NAME']}'s {season_str} season to {NBA_COLLECTIONS.PlayerSeasonStatsAdv}\n\n\n")
                bad_season_detected = True
                continue

        # if there was a bad season detected, we want to leave this in remaining IDs for post-processing
        if not bad_season_detected:
            remaining_ids.remove(player_id)

        print(f"[db.mongo.utils.update_mongo_player_game_logs] Successfully udpated {player['PLAYER_NAME']} in {NBA_COLLECTIONS.PlayerSeasonStatsAdv}")

    return remaining_ids


def update_mongo_team_season_stats(team_ids:list):

    # Get some of the collections we will be using
    collection = client[DATABASES.NBA][NBA_COLLECTIONS.TeamSeasonStats]
    collection_teams = client[DATABASES.NBA][NBA_COLLECTIONS.Teams]
    remaining_ids = copy.deepcopy(team_ids)

    # for each team id in the specified team_ids list...
    for team_id in tqdm(team_ids, desc='Getting season stats for all teams.', position=0, leave=True):
        
        team = collection_teams.find_one({'TEAM_ID': team_id})
        
        bad_season_detected = False # if we detect a bad season, this will get set to true, and the id won't be removed from remaining ids
        print(f"Getting {team['TEAM_NAME']}'s", end="")
        for year in range(int(team['YEAR_FOUNDED']), get_current_year()):
            
            # NBA.com only has data from 1996-97 season and beyond for this particular endpoint
            if year < 1996:
                continue
            try:
                # set up query year
                season_first_half = year
                season_second_half = get_season_str_YY(year+1)
                season_str = '{}-{}'.format(season_first_half, season_second_half)
                
                # query to check if this is already in mongo
                query = {"$and": [{"TEAM_ID": team['TEAM_ID']}, {'SEASON': season_str}]}
                already_in_collection = collection.find_one(query)

                # if it's not already there, we will try to add it
                if not already_in_collection:
                    print(f"...{season_str} season ", end="")
                    team_stats = TeamDashboardByGeneralSplits(team_id=team['TEAM_ID'], season=season_str, measure_type_detailed_defense=MeasureTypeDetailed.base, per_mode_detailed=PerModeDetailed.per_game).get_data_frames()[0] # 0 is overall (season long)
                    time.sleep(2)
                    if len(team_stats):
                        print(f"\n Updating with {season_str} data")
                        for k, row in team_stats.iterrows():
                            insert = dict(row)
                            keys = list(insert.keys())
                            # There are some junk columns we don't want to store
                            for key in keys:
                                if 'RANK' in key or 'CF' in key or 'sp_work' in key:
                                    insert.pop(key)
                            # Add some useful information
                            insert['TEAM_ID'] = team['TEAM_ID']
                            insert['SEASON'] = season_str
                            insert['TEAM_NAME'] = team['TEAM_NAME']
                            del insert['GROUP_VALUE']
                            del insert['GROUP_SET']
                            del insert['SEASON_YEAR']
                            collection.insert_one(insert)
                    else:
                        collection.insert_one({
                            "TEAM_ID": team['TEAM_ID'],
                            "SEASON": season_str,
                            "TEAM_NAME": team['TEAM_NAME']
                        })
            
            except:
                print(f"\n\n\n[db.mongo.utils.update_mongo_team_season_stats] COULDN'T ADD {team['TEAM_NAME']}'s {season_str} season to {NBA_COLLECTIONS.TeamSeasonStats}\n\n\n")
                bad_season_detected = True
                continue

        # if there was a bad season detected, we want to leave this in remaining IDs for post-processing
        if not bad_season_detected:
            remaining_ids.remove(team['TEAM_ID'])

        print(f"[db.mongo.utils.update_mongo_team_season_stats] Successfully udpated {team['TEAM_NAME']} in {NBA_COLLECTIONS.TeamSeasonStats}")

    return remaining_ids


def update_mongo_team_season_stats_adv(team_ids:list):

    # Get some of the collections we will be using
    collection = client[DATABASES.NBA][NBA_COLLECTIONS.TeamSeasonStatsAdv]
    collection_teams = client[DATABASES.NBA][NBA_COLLECTIONS.Teams]
    remaining_ids = copy.deepcopy(team_ids)

    # for each team id in the specified team_ids list...
    for team_id in tqdm(team_ids, desc='Getting advanced season stats for all teams.', position=0, leave=True):
        
        team = collection_teams.find_one({'TEAM_ID': team_id})
        
        bad_season_detected = False # if we detect a bad season, this will get set to true, and the id won't be removed from remaining ids
        print(f"Getting {team['TEAM_NAME']}'s", end="")
        for year in range(int(team['YEAR_FOUNDED']), get_current_year()):
            
            # NBA.com only has data from 1996-97 season and beyond for this particular endpoint
            if year < 1996:
                continue
            try:
                # set up query year
                season_first_half = year
                season_second_half = get_season_str_YY(year+1)
                season_str = '{}-{}'.format(season_first_half, season_second_half)
                
                # query to check if this is already in mongo
                query = {"$and": [{"TEAM_ID": team['TEAM_ID']}, {'SEASON': season_str}]}
                already_in_collection = collection.find_one(query)

                # if it's not already there, we will try to add it
                if not already_in_collection:
                    print(f"...{season_str} season ", end="")
                    team_stats_adv = TeamDashboardByGeneralSplits(team_id=team['TEAM_ID'], season=season_str, measure_type_detailed_defense=MeasureTypeDetailed.advanced, per_mode_detailed=PerModeDetailed.per_game).get_data_frames()[0] # 0 is overall (season long)
                    time.sleep(2)
                    if len(team_stats_adv):
                        print(f"\n Updating with {season_str} data")
                        for k, row in team_stats_adv.iterrows():
                            insert = dict(row)
                            keys = list(insert.keys())
                            # There are some junk columns we don't want to store
                            for key in keys:
                                if 'RANK' in key or 'CF' in key or 'sp_work' in key:
                                    insert.pop(key)
                            # Add some useful information
                            insert['TEAM_ID'] = team['TEAM_ID']
                            insert['SEASON'] = season_str
                            insert['TEAM_NAME'] = team['TEAM_NAME']
                            del insert['GROUP_VALUE']
                            del insert['GROUP_SET']
                            del insert['SEASON_YEAR']
                            collection.insert_one(insert)
                    else:
                        collection.insert_one({
                            "TEAM_ID": team['TEAM_ID'],
                            "SEASON": season_str,
                            "TEAM_NAME": team['TEAM_NAME']
                        })
            
            except:
                print(f"\n\n\n[db.mongo.utils.update_mongo_team_season_stats] COULDN'T ADD {team['TEAM_NAME']}'s {season_str} season to {NBA_COLLECTIONS.TeamSeasonStats}\n\n\n")
                bad_season_detected = True
                continue

        # if there was a bad season detected, we want to leave this in remaining IDs for post-processing
        if not bad_season_detected:
            remaining_ids.remove(team['TEAM_ID'])

        print(f"[db.mongo.utils.update_mongo_team_season_stats] Successfully udpated {team['TEAM_NAME']} in {NBA_COLLECTIONS.TeamSeasonStats}")

    return remaining_ids