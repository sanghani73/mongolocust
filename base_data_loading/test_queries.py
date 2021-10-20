#!/usr/bin/env python3

# This file is used purely to debug logic within Visual Studio Code. 
# It should not be used in any customer facing workshops
import os, sys
sys.path.append('resources/')
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from multiprocessing import Process
from pymongo import MongoClient
import datetime
from settings import DB_NAME, CLUSTER_URL, GROUP_1_PROCESSES, GROUP_2_PROCESSES, GROUP_3_PROCESSES
from faker import Faker

AUTH_COL = "authorisations"

# Global variables used in Main and Run functions
totalNumberOfProcesses = 0
processesList = []
totalDocs = 0

def run():
    client = MongoClient(CLUSTER_URL)
    db = client[DB_NAME]
    coll = db[AUTH_COL]
    faker = Faker()

    # Generate a random date to select from the collection
    search_date = faker.date_time_between(start_date='-2y', end_date='-1y')

    # find a random document using the date, start and end times    
    start_time = datetime.datetime(search_date.year, search_date.month, search_date.day, 4, 0, 0)
    end_time = datetime.datetime(search_date.year, search_date.month, search_date.day, 10, 0, 0)

    result = coll.find_one({
                            'posting_date': { "$gte": start_time , "$lt": end_time } 
                            })
    print(result)

if __name__ == '__main__':
    run()