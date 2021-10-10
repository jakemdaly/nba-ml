import sys
sys.path.append(r"/home/jakemdaly/Documents/GitRepos/nba-ml")
import pymongo
from tqdm import tqdm

from nba.utils import get_fantasy_points_from_game
from db.mongo.populate_utils import *
from db.mongo.enums import *
from db.mongo.utils import get_season_str_after, has_both_years

nba_db = client['NBA']

def create_minutes_dataset_from_basic_and_advanced_player_seasons():
    '''
    This function will create a collection in mongo that has basic and advanced stats for a player season, and appends to this the MINUTES PER GAME for that
    players next season.
    '''
    # collections that we will be using
    pss = nba_db.PlayerSeasonStats
    pssa = nba_db.PlayerSeasonStatsAdv
    PlayerSeasonDB = nba_db.MinutesDataset

    # the list of basic and advanced stats that we wish to keep
    season_stat_keys = ['GP', 'PLAYER_AGE', 'W_PCT', 'MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PTS', 'PLUS_MINUS', 'OFF_RATING', 'DEF_RATING', 'NET_RATING', 'AST_PCT', 'AST_TO', 'AST_RATIO', 'OREB_PCT', 'DREB_PCT', 'REB_PCT', 'EFG_PCT', 'TS_PCT', 'USG_PCT', 'PACE','PIE']

    # get all ids that are in either PlayerSeasonStats and/or PlayerSeasonStatsAdv
    ids = set(pss.distinct("PLAYER_ID")) | set(pssa.distinct("PLAYER_ID"))

    # Loop over every player
    for pid in tqdm(ids):

        # Get the seasons where this player played at least 15 games
        ss_basic_years = pss.distinct("SEASON", {"$and": [{"PLAYER_ID": pid},{"GP": {"$gte":15}}]})
        ss_adv_years = pss.distinct("SEASON", {"$and": [{"PLAYER_ID": pid},{"GP": {"$gte":15}}]})

        # only need to iterate over one of these lists because we need to have it in each in order to be able to use it
        for season in ss_basic_years:

            # check if this player has a BASIC and ADVANCED season stats entry for both this season AND next season
            if not has_both_years(season, ss_basic_years, ss_adv_years):
                continue

            # if this is true, get these documents
            bas = [doc for doc in pss.find({"$and": [{"PLAYER_ID": pid},{"GP": {"$gte":15}}, {"SEASON": season}]})]
            adv = [doc for doc in pssa.find({"$and": [{"PLAYER_ID": pid},{"GP": {"$gte":15}}, {"SEASON": season}]})]
            # this will be used to get the player's next season minutes from
            bas_next = [doc for doc in pss.find({"$and": [{"PLAYER_ID": pid},{"GP": {"$gte":15}}, {"SEASON": get_season_str_after(season)}]})]
            
            # if we have both, we should have only one season where this is true
            if adv and bas:
                assert len(adv)==1
                assert len(bas)==1
                
                # put the basic and advanced dictionaries into a combined one, and add the entry for NEXT season's minutes
                combined_dict = {**bas[0], **adv[0]}
                combined_dict = {key: value for key, value in combined_dict.items() if key in season_stat_keys}
                combined_dict.update({"MIN_NEXT_SEASON": bas_next[0]["MIN"]})

                # update db
                PlayerSeasonDB.insert_one(combined_dict)
    
    print("Done building minutes dataset")


def create_player_game_log_dataset():
    '''
    This function will create a collection in mongo that has basic and advanced game log stats for all players.
    '''
    def pop_sibling(gl_adv:list, game_id):
        '''Loops over gl_adv, and once it finds an item with GAME_ID==game_id, removes it and returns the item. If it can't find it, it returns None'''
        for gla in gl_adv:
            if gla['GAME_ID']==game_id:
                gl_adv.remove(gla)
                return gla
        return None

    # collections that we will be using
    PGL = nba_db.PlayerGameLogs
    PGLA = nba_db.PlayerGameLogsAdv
    Dataset = nba_db.DatasetPlayerGameLogs

    # the list of basic and advanced stats that we wish to keep
    season_stat_keys = ['PLAYER_NAME', 'PLAYER_ID', 'GAME_DATE', 'GAME_ID', 'START_POSITION', 'PLAYER_AGE', 'MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'PF', 'PTS', 'PLUS_MINUS', 'OFF_RATING', 'DEF_RATING', 'NET_RATING', 'OREB_PCT', 'DREB_PCT', 'AST_PCT', 'EFG_PCT', 'TS_PCT', 'USG_PCT', 'PACE','PIE', 'POSS']

    # get all ids that are in either PlayerSeasonStats and/or PlayerSeasonStatsAdv
    ids = set(PGL.distinct("PLAYER_ID")) | set(PGLA.distinct("PLAYER_ID"))

    # Loop over every player
    for pid in tqdm(ids, desc="Building player game log dataset. Looping over game logs of all players..."):

        # Get the seasons where this player played at least 15 games
        gl_basic = [doc for doc in PGL.find({"PLAYER_ID": pid}).sort('GAME_DATE',pymongo.ASCENDING)]
        gl_adv   = [doc for doc in PGLA.find({"PLAYER_ID": pid}).sort('GAME_DATE', pymongo.ASCENDING)]
        combined = []

        for glb in gl_basic:
            gid = glb['GAME_ID']
            gla = pop_sibling(gl_adv, gid)
            if not gla:
                continue

            combined_dict = {**gla, **glb}
            combined_dict = {key: value for key, value in combined_dict.items() if key in season_stat_keys}
            combined_dict['START_F'], combined_dict['START_C'], combined_dict['START_G'] = 0, 0, 0
            if combined_dict['START_POSITION']=='F':
                combined_dict['START_F']=1
            elif combined_dict['START_POSITION']=='C':
                combined_dict['START_C']=1
            elif combined_dict['START_POSITION']=='G':
                combined_dict['START_G']=1
            del combined_dict['START_POSITION']
            combined_dict['FANTASY_POINTS_v0'] = get_fantasy_points_from_game(glb)

            combined.append(combined_dict)
        
        if combined:
            Dataset.insert_many(combined)

    print("Done building player game log dataset")