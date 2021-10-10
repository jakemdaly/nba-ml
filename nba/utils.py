class FRules_v0:

    # Stat Categories that will yield fantasy points
    FGM = 1
    FGA = -1
    FTM = 1
    FTA = -1
    REB = 1
    AST = 1
    STL = 1.5
    BLK = 1.5
    TOV = -1
    EJ = -1
    TD = 1
    PTS = 1


def get_fantasy_points_from_game(player_game_log, fantasy_rules=FRules_v0):
    '''Will return the fantasy points a player scored from a game log, without EJ because this is a fine approximation'''

    # positive points
    TD = fantasy_rules.TD if ((player_game_log["PTS"]>9) and (player_game_log["REB"]>9) and (player_game_log["AST"]>9)) else 0
    PTS = fantasy_rules.PTS * player_game_log["PTS"]
    FGM = fantasy_rules.FGM * player_game_log["FGM"]
    FTM = fantasy_rules.FTM * player_game_log["FTM"]
    REB = fantasy_rules.REB * player_game_log["REB"]
    AST = fantasy_rules.AST * player_game_log["AST"]
    STL = fantasy_rules.STL * player_game_log["STL"]
    BLK = fantasy_rules.BLK * player_game_log["BLK"]
    
    # negative points
    FGA = fantasy_rules.FGA * player_game_log["FGA"]
    FTA = fantasy_rules.FTA * player_game_log["FTA"]
    TOV = fantasy_rules.TOV * player_game_log["TOV"]

    # because "negative" is accounted for in fantasy_rules, all we have to do is add everything here
    fantasy_points = TD + PTS + FGM + FTM + REB + AST + STL + BLK + FGA + FTA + TOV

    return fantasy_points