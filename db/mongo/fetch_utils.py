from pymongo import MongoClient
from pymongo.database import Database

client = MongoClient('localhost', 27017)

def get_all_player_ids():
    '''Returns a list of all the player ids from Mongo'''
    return client['NBA']['Players'].distinct("id")