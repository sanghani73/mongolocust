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
        year = 2020
        month = 6
        day = random.randint(1, 30)
        start_hr = random.randint(0, 22)
        end_hr = start_hr+1
        merch_id = 30001
        alliance_code = 'CODE123'

        # define search query
        search = {
            '$search': {
                'compound': {
                    'must': [
                        {   'range': {
                                'path': 'posting_date',
                                'gte': datetime(year, month, day, start_hr, 0, 0, tzinfo=timezone.utc), 
                                'lte': datetime(year, month, day, end_hr, 0, 0, tzinfo=timezone.utc)
                                }
                        },
                        {   'range': {
                                'path': 'merch_id',
                                'gte': merch_id,
                                'lte': merch_id
                                }
                        },
                        {   'text':{
                                'path': 'alliance_code',
                                'query': alliance_code
                                }
                        },
                        {   'near': {
                                'path': 'posting_date',
                                'origin': datetime(year, month, day, end_hr, 0, 0, tzinfo=timezone.utc),
                                'pivot': 1
                                }
                        }
                    ],
                'filter': [
                    {   'range': {
                            'path':'transaction_details.purchase_amount',
                            'gte': 998.84,
                            'lte': 998.84
                            }
                    },
                    {   'text': {
                            'path': 'merchant_details.merch_type',
                            'query': 'DUGVIK'
                            }
                    }
                    ]
                }
            }
        }

        # rename the _id to city
        limit = {'$limit': 50}

        pipeline = [search, limit]

        # make sure we fetch everything by explicitly casting to list
        # use self.collection instead of self.collection_secondary to run the pipeline on the primary
        return list(self.authorisation_collection.aggregate(pipeline))

    @task(weight=1)
    def do_find_authorisation(self):
        self._process('find-authorisation-1', self.find_authorisation)

    # @task(weight=1)
    # def do_insert_authorisation(self):
    #     self._process('insert-authorisation', self.insert_authorisation)

    # @task(weight=1)
    # def do_update_authorisation(self):
    #     self._process('update-authorisation', self.update_authorisation)

