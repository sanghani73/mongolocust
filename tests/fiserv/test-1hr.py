from datetime import datetime, timezone
from locust import task, between

from mongo_user import MongoUser
import random
import generate_authorisation

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

    def on_start(self):
        """
        Executed every time a new test is started - place init code here
        """
        # prepare the collection
        self.authorisation_collection = self.ensure_collection('authorisations')
        self.auth_id_cache = []

    def insert_authorisation(self):
        document = generate_authorisation.Authorisation.generate_authorisation_doc(3)

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
        self.authorisation_collection.update_one({'auth_id': auth_id}, {
                '$currentDate': {'updates.last_updated': { '$type': 'date' }},
                '$set': {'updates.some_field': 'some_value'}
                })

    def find_authorisation(self):
        # Find a random day & time to search
        day = random.randint(1, 30)
        start_hr = random.randint(0, 22)
        end_hr = start_hr+1
        merch_id = random.randint(30001, 30002)
        alliance_code = 'CODE123'

        filter={
            'merch_id': merch_id, 
            'alliance_code': alliance_code, 
            'posting_date': {
                '$gte': datetime(2020, 6, day, start_hr, 0, 0, tzinfo=timezone.utc), 
                '$lte': datetime(2020, 6, day, end_hr, 0, 0, tzinfo=timezone.utc)
            }
        }
        sort=list({
            'posting_date': -1
        }.items())
        # fetch a random page of data
        skip=random.randint(0,10)
        limit=50

        # find a random document using an the auth id and update it
        return list(self.authorisation_collection.find(
            filter=filter,
            sort=sort,
            skip=skip,
            limit=limit
        ))




    @task(weight=1)
    def do_find_authorisation(self):
        self._process('find-authorisation-1', self.find_authorisation)

    # @task(weight=1)
    # def do_insert_authorisation(self):
    #     self._process('insert-authorisation', self.insert_authorisation)

    # @task(weight=1)
    # def do_update_authorisation(self):
    #     self._process('update-authorisation', self.update_authorisation)

