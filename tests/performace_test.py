from datetime import datetime
from locust import task, between

from mongo_user import MongoUser
import random

# number of cache entries for updates and queries
IDS_TO_CACHE = 1000000

class MongoSampleUser(MongoUser):
    """
    Generic sample mongodb workload generator
    """
    # no delays between operations
    wait_time = between(0.0, 0.0)

    def __init__(self, environment):
        super().__init__(environment)
        self.auth_id_cache = []
        self.authorisation_collection = None

    def generate_new_authorisation(self):
        """
        Generate an authorisation document
        """
        document = {
            'auth_id': self.faker.uuid4(),
            'merch_id': self.faker.random_int(min=901000, max=901030),
            'alliance_code': self.faker.lexify(text='????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
            'bank_name': self.faker.lexify(text='???? PLC.', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
            'bank_country': self.faker.country(),
            'posting_date': self.faker.date_time_between(start_date='-2y', end_date='-1y'),
            'merchant_detals': {
                'external_merch_id': self.faker.random_int(min=100000, max=999999),
                'custom_merch_id': self.faker.random_int(min=100000, max=999999),
            }
        }
        return document

    def on_start(self):
        """
        Executed every time a new test is started - place init code here
        """
        # prepare the collection
        self.authorisation_collection = self.ensure_collection('authorisations', '')
        self.auth_id_cache = []

    def insert_authorisation(self):
        document = self.generate_new_authorisation()

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

    @task(weight=1)
    def do_find_authorisation(self):
        self._process('find-authorisation-1', self.find_authorisation)

    @task(weight=1)
    def do_insert_authorisation(self):
        self._process('insert-authorisation', self.insert_authorisation)

    @task(weight=1)
    def do_update_authorisation(self):
        self._process('update-authorisation', self.update_authorisation)

