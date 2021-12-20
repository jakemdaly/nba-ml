from tqdm import tqdm
from datetime import date, datetime
from .fetch_utils import *

client = MongoClient('localhost', 27017)

def get_current_year():
    today = date.today()
    return today.year

def get_season_str_YY(year_that_needs_converting_int):
    '''Converts a 4 digit int year to a two digit stream like 1999 -> '99' '''
    assert isinstance(year_that_needs_converting_int, int)
    year_str = str(year_that_needs_converting_int)[2:]
    # NEED TO RETURN AS STRING BECAUSE CAN'T RETURN 00 as int
    return year_str

def get_season_str_after(season:str):
    '''For example, if season=='2001-02', will return 2002-03 '''
    season_first_part = int(season.split("-")[0])
    season_first_part = str(season_first_part+1)
    season_second_part = int(season.split("-")[1])
    season_second_part = str(season_second_part+1) if season_second_part >= 9 else '0'+str(season_second_part+1)
    season_second_part = '00' if season_second_part == '100' else season_second_part
    return season_first_part+'-'+season_second_part

def min_to_float(min_string: str):
    '''MM:SS to float. Eg. '14:30' to 14.5'''
    if min_string == None:
        return 0
    else:
        split = min_string.split(':')
        return float(split[0]) + float(split[1])/60


def get_winning_team(box_score_summary_line_score):
    '''This will compute the winning team from the BoxScoreSummaryV2's LineScore'''
    team1 = box_score_summary_line_score[0]['TEAM_ABBREVIATION']
    score_team1 = box_score_summary_line_score[0]['PTS']
    team2 = box_score_summary_line_score[1]['TEAM_ABBREVIATION']
    score_team2 = box_score_summary_line_score[1]['PTS']
    if score_team1 > score_team2:
        return team1
    else:
        return team2


def has_both_years(season, bas_years, adv_years):
    '''
    Will check if we can proceed with using this player season for training by checking that we have the both the advanced and basic 
    season stats for a given season AND the next season (required for the targets/labels)
    Args:
        season (str) : "YYYY-YY" like 1999-00
        bas_years (list) : list of the basic season stats we want to check for year and year+1 in
        adv_years (list) : list of the adv season stats we want to check for year and year + 1 in

    Returns:
        bool
    '''
    y_int = int(season.split("-")[0])
    bas_years_int = [int(y.split("-")[0]) for y in bas_years]
    adv_years_int = [int(y.split("-")[0]) for y in adv_years]
    if y_int not in bas_years_int:
        return False
    if y_int not in adv_years_int:
        return False
    if (y_int + 1) not in bas_years_int:
        return False
    if (y_int + 1) not in adv_years_int:
        return False
    return True

def add_age_to_player_season():
    '''
    Will use CPI to calculate a players age for a given season, and add this age to a player season stats collection
    '''
    collection_cpi = client['NBA']['CommonPlayerInfo']
    collection_player_seasons = client['NBA']['PlayerSeasonStats']
    ids = get_all_player_ids()
    for pid in ids:
        cpi = collection_cpi.find_one({"PLAYER_ID": pid})
        birthdate = cpi['BIRTHDATE'].split("T")[0]

        for player_season in collection_player_seasons.find({"PLAYER_ID": pid}):

            year_str = player_season["SEASON"].split("-")[0] + " 10 15" # in general season starts around mid October, so we will make the 
            # year string of the format "2018 10 15" for more accurate age at beginning of season than simply "2018"
            
            delta = datetime.strptime(year_str, "%Y %m %d") - datetime.strptime(birthdate, "%Y-%m-%d")
            age = delta.days/365.25
            player_season['PLAYER_AGE'] = age

            collection_player_seasons.replace_one({"_id": player_season['_id']}, player_season)


def add_age_to_player_gamelog():
    '''
    Will use CPI to calculate a players age for a given game, and add this age to a player season stats collection
    '''
    collection_cpi = client['NBA']['CommonPlayerInfo']
    collection_gamelogs = client['NBA']['PlayerGameLogs']
    ids = get_all_player_ids()
    for pid in tqdm(ids, desc="Adding age to player game logs..."):
        cpi = collection_cpi.find_one({"PLAYER_ID": pid})
        birthdate = cpi['BIRTHDATE'].split("T")[0]
        
        for player_gamelog in collection_gamelogs.find({"PLAYER_ID": pid, "PLAYER_AGE": {"$exists": False}}):

            if 'PLAYER_AGE' not in player_gamelog:

                date = player_gamelog["GAME_DATE"] # Format is "APR 20, 1997"
                
                delta = date - datetime.strptime(birthdate, "%Y-%m-%d")
                age = delta.days/365.25
                player_gamelog['PLAYER_AGE'] = age

                collection_gamelogs.replace_one({"_id": player_gamelog['_id']}, player_gamelog)

def convert_collection_datestring_to_datetime(collection=client.NBA.PlayerGameLogs, datestringname:str='GAME_DATE', datestringformat="%b %d, %Y"):

    for doc in tqdm(collection.find({}), desc="Converting dates to datetime format"):
        assert datestringname in doc
        _id = doc['_id']
        date = doc[datestringname]
        try: 
            datetimeformat = datetime.strptime(date, datestringformat)
        except ValueError:
            raise ValueError(f"Incorrect data format, should be {datestringformat}")
        doc[datestringname] = datetimeformat
        collection.replace_one({"_id": _id}, doc)