import sys, copy, time, re

from threading import local
from pytz import timezone
from datetime import date, datetime
from tqdm import tqdm
from pymongo import MongoClient
from draft_kings import Sport, Client
import pandas as pd

sys.path.append(r"/home/jakemdaly/Documents/GitRepos/nba-ml")
from dfs.injury_scraper import scrape

dkClient = Client()
client = MongoClient('localhost', 27017)

# Useful regexs
and_1_regex = r'NBA \$[0-9]{1,3}K And-One \[20 Entry Max\]' # And-One Contest regex

def get_draft_kings_available_players(contest_name_regex=and_1_regex):
    contests = dkClient.contests(sport=Sport.NBA).contests
    for contest in contests:
        if re.findall(contest_name_regex, contest.name):
            injury_report_df = scrape()
            players = []
            draft_group_id = contest.draft_group_id
            tries_remaining = 5
            while tries_remaining:
                try:
                    dk_players_list = dkClient.available_players(draft_group_id=draft_group_id).players
                    print("Successfully retreived available players from DK contest.")
                except ConnectionError as e:
                    tries_remaining -= 1
                    wait = 5
                    print(f"Couldn't get available players. Will try {tries_remaining} more times, starting in {wait} seconds.")
                    time.sleep(5)
                    if tries_remaining > 0:
                        continue
                break
            injured_players = list(injury_report_df.Player)
            for dk_player in tqdm(dk_players_list, desc=f'Compiling player information for DK contest \'{contest.name}\''):
                full_name = dk_player.first_name + ' ' + dk_player.last_name
                local_db_player = [p for p in client.NBA.Players.find({'PLAYER_NAME': full_name, "IS_ACTIVE": True})]
                if len(local_db_player) > 1:
                    print(f"Found > 1 active players in Mongo.NBA.Players with name={full_name}. Exiting.")
                    input() # virtual break point
                    exit()
                elif len(local_db_player)==1:
                    # local_db_player = local_db_player[0]
                    pid = local_db_player[0]['PLAYER_ID']
                else:
                    pid = None # means the player might be a valid DraftKings choice, but he has not been logged into our infrastructure yet. That's ok
                if full_name in injured_players:
                    injury_status = injury_report_df[injury_report_df.Player==full_name].Status.item()
                else:
                    injury_status = ''
                
                home_game = bool(dk_player.team_series_details.home_team_id == dk_player.team_id)

                players.append( {
                    'PLAYER_NAME': convert_name(full_name),
                    'TEAM_ABB': dk_player.team_id,
                    'OPP_TEAM_ABB': dk_player.team_series_details.away_team_id if home_game else dk_player.team_series_details.home_team_id,
                    'GAME_LOCATION': dk_player.team_series_details.home_team_id,
                    'PLAYER_ID': pid,
                    'DK_PLAYER_ID': dk_player.player_id,
                    'SALARY': dk_player.draft_details.salary,
                    'FPPG': dk_player.points_per_game,
                    'POS_DK_NAME': dk_player.position_details.name,
                    'POS_DK_ID': dk_player.position_details.position_id,
                    'OPP_POS_RANK': dk_player.team_series_details.opposition_rank,
                    'INJURY': injury_status,
                    'GAME_DATE': convert_time(contest.starts_at)
                })

            return players

    # Couldn't find anything, exit
    print("Couldn't find contest matching the selected regex")
    exit()        

def update_dk_player_game_infos(list_of_players):
    '''Will update the mongo collection NBA.DKPlayerGameInfos with the list of player game infos returned by @get_draft_kings_available_players'''
    
    def already_in_collection(entry):
        if entry['PLAYER_ID']==None:
            result = [doc for doc in client.NBA.DKPlayerGameInfos.find({'PLAYER_NAME': entry['PLAYER_NAME'], 'GAME_DATE': entry['GAME_DATE']})]
        else:
            result = [doc for doc in client.NBA.DKPlayerGameInfos.find({'PLAYER_ID': entry['PLAYER_ID'], 'GAME_DATE': entry['GAME_DATE']})]
        if len(result) > 1:
            print(f"Found more than one entry already in collection that matches this player id ({entry['PLAYER_ID']}) and game date ({entry['GAME_DATE']})")
            input() # virtual break point
            exit()
        elif len(result) == 1:
            return True
        elif len(result) == 0:
            return False

    print("Updating NBA.DKPlayerGameInfos with each players DFS info.")
    for dkplayer in tqdm(list_of_players):
        if not already_in_collection(dkplayer):
            client.NBA.DKPlayerGameInfos.insert_one(dkplayer)

def update_dk_player_game_infos_from_csv(path_to_csv):
    ''''''
    def already_in_collection(entry):
        if entry['PLAYER_ID']==None:
            result = [doc for doc in client.NBA.DKPlayerGameInfos.find({'PLAYER_NAME': entry['PLAYER_NAME'], 'GAME_DATE': entry['GAME_DATE']})]
        else:
            result = [doc for doc in client.NBA.DKPlayerGameInfos.find({'PLAYER_ID': entry['PLAYER_ID'], 'GAME_DATE': entry['GAME_DATE']})]
        if len(result) > 1:
            print(f"Found more than one entry already in collection that matches this player id ({entry['PLAYER_ID']}) and game date ({entry['GAME_DATE']})")
            input() # virtual break point
            exit()
        elif len(result) == 1:
            return True
        elif len(result) == 0:
            return False

    df = pd.read_csv(path_to_csv)
    players = []
    for i, row in df.iterrows():
        full_name = row['Name']
        local_db_player = [p for p in client.NBA.Players.find({'PLAYER_NAME': full_name, "IS_ACTIVE": True})]
        if len(local_db_player) > 1:
            print(f"Found > 1 active players in Mongo.NBA.Players with name={full_name}. Exiting.")
            input() # virtual break point
            exit()
        elif len(local_db_player)==1:
            # local_db_player = local_db_player[0]
            pid = local_db_player[0]['PLAYER_ID']
        else:
            pid = None # means the player might be a valid DraftKings choice, but he has not been logged into our infrastructure yet. That's ok
        pos = row['Roster Position']
        home_game = bool(row['TeamAbbrev'] == row['Game Info'].split("@")[1][:3])
        home_team = row['Game Info'].split("@")[1][:3]
        away_team = row['Game Info'].split("@")[0][:3]
        players.append( {
            'PLAYER_NAME': convert_name(row['Name']),
            'TEAM_ABB': row['TeamAbbrev'],
            'OPP_TEAM_ABB': away_team if home_game else home_team,
            'GAME_LOCATION': home_team,
            'PLAYER_ID': pid,
            'DK_PLAYER_ID': row['ID'],
            'SALARY': row['Salary'],
            'FPPG': row['AvgPointsPerGame'],
            'POS_DK_NAME': row['Roster Position'].replace("/F", "").replace("/G", "").replace("/UTIL", ""),
            'POS_DK_ID': -1,
            'OPP_POS_RANK': -1,
            'INJURY': '?', # If it's from a historical csv, we unfortunately don't know the injury status from that day
            'GAME_DATE': datetime.strptime(row['Game Info'].split(' ')[1], "%m/%d/%Y")
        })

    print(f"Updating NBA.DKPlayerGameInfos from {path_to_csv}")
    for dkplayer in players:
        if not already_in_collection(dkplayer):
            client.NBA.DKPlayerGameInfos.insert_one(dkplayer)


def update_latest_injury_report_mongo():

    upgraded = []
    downgraded = []

    today = convert_time(datetime.now())
    injury_report_df = scrape()
    injured_players_converted_names = {convert_name(p): p for p in injury_report_df.Player}
    for dk_doc in client.NBA.DKPlayerGameInfos.find({'GAME_DATE': today}):
        status = dk_doc['INJURY']
        if dk_doc['PLAYER_NAME'] not in injured_players_converted_names:
            
            # If this player is not in the injured players list, but previously had an injury, upgrade him
            if status != '':
                upgraded.append((dk_doc['PLAYER_NAME'], status, ''))
                dk_doc['INJURY'] = ''
                client.NBA.DKPlayerGameInfos.replace_one({"_id": dk_doc['_id']}, dk_doc)

        else:
            new_status = injury_report_df[injury_report_df.Player==injured_players_converted_names[dk_doc['PLAYER_NAME']]].Status.item()
            if status != new_status:
                dk_doc['INJURY'] = new_status
                client.NBA.DKPlayerGameInfos.replace_one({"_id": dk_doc['_id']}, dk_doc)
                for key,value in STATUS_CONVERSION_MATRIX.items():
                    if status==key:
                        change = STATUS_CONVERSION_MATRIX[status][new_status]
                        if change == 'Upgrade':
                            upgraded.append((dk_doc['PLAYER_NAME'], status, new_status))
                        elif change == 'Downgrade':
                            downgraded.append((dk_doc['PLAYER_NAME'], status, new_status))

    print("---------- UPGRADES ----------")
    for update in upgraded:
        print(f"{update[0]:<25} ... {update[1]:>15} --> {update[2]:<15}")

    print("--------- DOWNGRADES ---------")
    for update in downgraded:
        print(f"{update[0]:<25} ... {update[1]:>15} --> {update[2]:<15}")



STATUS_CONVERSION_MATRIX = {
    'Sidelined': {
        'Doubtful': 'Upgrade',
        'Questionable': 'Upgrade',
        'Probable': 'Upgrade'
    },
    'Doubtful':{
        'Sidelined': 'Downgrade',
        'Questionable': 'Upgrade',
        'Probable': 'Upgrade'
    },
    'Questionable': {
        'Sidelined': 'Downgrade',
        'Doubtful': 'Downgrade',
        'Probable': 'Upgrade'
    },
    'Probable': {
        'Sidelined': 'Downgrade',
        'Doubtful': 'Downgrade',
        'Questionable': 'Downgrade'
    }
}

TEAMID_TO_ABB = {
    1: "ATL",
    2: "BOS",
    3: "NOP",
    4: "CHI",
    5: "CLE",
    6: "DAL",
    7: "DEN",
    8: "",
    9: "GSW",
    10: "HOU",
    11: "IND",
    12: "LAC",
    13: "LAL",
    14: "MIA",
    15: "MIL",
    16: "MIN",
    17: "BKN",
    18: "NYK",
    19: "ORL",
    20: "PHI",
    21: "PHX",
    22: "POR",
    23: "SAC",
    24: "SAS",
    25: "OKC",
    26: "UTA",
    27: "WAS",
    28: "TOR",
    29: "MEM",
    30: "",
    5312: "CHA"
}

DKPLAYERNAME_TO_NBAAPIPLAYERNAME = {
    "Guillermo Hernangomez": "Willy Hernangomez",
    "Moe Harkless": "Maurice Harkless",
    "PJ Washington": "P.J. Washington",
    "KJ Martin": "Kenyon Martin Jr.",
    "Cameron Thomas": "Cam Thomas",
    "Greg Brown": "Greg Brown III",
    "Robert Williams": "Robert Williams III",
    "Enes Freedom": "Enes Kanter"
}

def update_dkplayergameinfosteams_with_abb():

    for doc in client.NBA.DKPlayerGameInfos.find({}):
        if isinstance(doc['TEAM_ABB'], int):
            doc['TEAM_ABB'] = TEAMID_TO_ABB[doc['TEAM_ABB']]
        if isinstance(doc['OPP_TEAM_ABB'], int):
            doc['OPP_TEAM_ABB'] = TEAMID_TO_ABB[doc['OPP_TEAM_ABB']]
        if isinstance(doc['GAME_LOCATION'], int):
            doc['GAME_LOCATION'] = TEAMID_TO_ABB[doc['GAME_LOCATION']]
        client.NBA.DKPlayerGameInfos.replace_one({"_id": doc["_id"]}, doc)


def convert_name(dkplayername):
    if dkplayername in DKPLAYERNAME_TO_NBAAPIPLAYERNAME.keys():
        return DKPLAYERNAME_TO_NBAAPIPLAYERNAME[dkplayername]
    else:
        return dkplayername

def convert_time(time):
    '''Converts a UCT time to what it would be in pacific'''
    pst = time.astimezone(timezone('US/Pacific'))
    return datetime(pst.year, pst.month, pst.day)