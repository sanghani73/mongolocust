from datetime import datetime, timedelta
from locust import task, between
from faker import Faker
from random import randrange
import pymongo

from mongo_user import MongoUser
import random
import generate_authorisation

# number of cache entries for updates and queries
IDS_TO_CACHE = 1000000
ALLIANCE_CODE = "CODE123"

class MongoSampleUser(MongoUser):
    """
    Generic sample mongodb workload generator
    """
    # no delays between operations
    wait_time = between(0.0, 0.0)

    def generate_random_date(start, end):
        delta = end - start
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        return start + timedelta(seconds=random_second)  

    def __init__(self, environment):
        super().__init__(environment)
        self.auth_id_cache = []
        self.authorisation_collection = None

    def on_start(self):
        """
        Executed every time a new test is started - place init code here
        """
        # prepare the collection
        self.authorisation_collection = self.ensure_collection('authorisations')
        self.auth_id_cache = []

    def insert_authorisation(self):
        document = generate_authorisation.Authorisation.generate_new_grouped_authorisation_doc(10000, 10100)

        self.auth_id_to_cache = (document['auth_id'])
        if len(self.auth_id_cache) < IDS_TO_CACHE:
            self.auth_id_cache.append(self.auth_id_to_cache)
        else:
            # randomly update one of the cached entries so we're not always working on the same ids
            if random.randint(0, 9) == 0:
                self.auth_id_cache[random.randint(0, len(self.auth_id_cache) - 1)] = self.auth_id_to_cache

        self.authorisation_collection.insert_one(document)

    def update_authorisation(self):
        # Find a random auth id to update
        if not self.auth_id_cache:
            return

        # find a random document using an the auth id and update it
        auth_id = random.choice(self.auth_id_cache)
        # self.collection.find_one({'first_name': cached_names[0], 'last_name': cached_names[1]})
        self.authorisation_collection.update_one({'auth_id': auth_id}, {
                '$currentDate': {'updates.last_updated': { '$type': 'date' },
                '$set': {'updates.some_field': 'some_value'}}
                })

    def find_authorisation(self):
        # Find a random auth id to update
        if not self.auth_id_cache:
            return

        # find a random document using an the auth id and update it
        auth_id = random.choice(self.auth_id_cache)
        self.authorisation_collection.find_one({'auth_id': auth_id})

    def find_authorisation_by_start_and_end_date(self, merch_id, start_time, end_time):
        self.authorisation_collection.find({
                                            'merch_id': merch_id, 
                                            'alliance_code': ALLIANCE_CODE, 
                                            'posting_date': { "$gte": start_time , "$lt": end_time } 
                                            })

    @task(weight=1)
    # Find all authorisations on given date between given times for a single merchant & alliance code
    def do_find_authorisation_on_given_day_between_given_times(self):
        faker = Faker()
        merch_id = "30001"

        # Generate a random date to select from the collection
        search_date = faker.date_time_between(start_date='-2y', end_date='-1y')
        start_time = datetime.datetime(search_date.year, search_date.month, search_date.day, 9, 0, 0)
        end_time = datetime.datetime(search_date.year, search_date.month, search_date.day, 10, 0, 0)

        self._process('find-authorisation-by-date', self.find_authorisation_by_date(merch_id, start_time, end_time))

    @task(weight=1)
    # authorisations that took place in a single day for a single merchant & alliance code
    def do_find_authorisation_on_single_day(self):
        faker = Faker()
        merch_id = "30002"

        # Generate a random date to select from the collection
        search_date = faker.date_time_between(start_date='-2y', end_date='-1y')
        start_time = datetime.datetime(search_date.year, search_date.month, search_date.day, 0, 0, 0)
        end_time = datetime.datetime(search_date.year, search_date.month, search_date.day, 23, 59, 59, 9999)

        self._process('find-authorisation-by-date', self.find_authorisation_by_date(merch_id, start_time, end_time))

    # @task(weight=1)
    # def do_insert_authorisation(self):
    #     self._process('insert-authorisation', self.insert_authorisation)

    # @task(weight=1)
    # def do_update_authorisation(self):
    #     self._process('update-authorisation', self.update_authorisation)

