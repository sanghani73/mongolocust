from locust import task, between
from mongo_user import MongoUser

import pymongo
import random


# docs to insert per batch insert
DOCS_PER_BATCH = 1000


class MongoSampleUser(MongoUser):
    """
    Generic sample mongodb workload generator
    """
    # no delays between operations
    wait_time = between(0.0, 0.0)

    def __init__(self, environment):
        super().__init__(environment)
        self.authorisation_collection = None
        self.clearing_collection = None

    def generate_new_clearing(self):
        """
        Generate a clearing document
        """
        document = {
            'clearing_id': self.faker.uuid4(),
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

    # "auth_id": "$objectid",
    # "merch_id": {"$number": {"min": 901000, "max": 901030}},
    # "alliance_code" : {"$string": {"length": 4, "symbols": false, "alpha": true, "casing": "upper"}},
    # "bank_name": "bank_name",
    # "bank_country": "$country",
    # "posting_date" : {"$date": {"min": "2020-01-01", "max": "2020-12-31"}},
    # "merchant_details": {
    #     "external_merch_id": {"$number": {"min": 100000, "max": 1000000}},
    #     "custom_merch_id": {"$number": {"min": 100000, "max": 1000000}},
    #     "merch_type": {"$string": {"length": 6, "symbols": false, "alpha": true, "casing": "upper"}},
    #     "name": "$company",
    #     "trade_name": "$company",
    #     "mcc_iso": {"$number": {"min": 100000, "max": 1000000}},
    #     "partner_code": {"$join": {"array": [{"$string": {"length": 3, "symbols": false, "alpha": true, "casing": "upper"}}, {"$number": {"min":10000, "max":20000}}]}}
    # },

    # def insert_single_document(self):
    #     document = self.generate_test_doc()
    #     # cache the first_name, last_name tuple for queries
    #     self.authorisation_collection.insert_one(document)

    def on_start(self):
        """
        Executed every time a new test is started - place init code here
        """
        # prepare the collection
        self.authorisation_collection = self.ensure_collection('authorisations', '')
        self.clearing_collection = self.ensure_collection('clearing', '')

    # @task(weight=1)
    # def do_insert_document(self):
    #     self._process('insert-document', self.insert_single_document)

    @task(weight=1)
    def do_insert_auth_bulk(self):
        self._process('insert-authorisations-in-bulk', lambda: self.authorisation_collection.insert_many(
            [self.generate_new_authorisation() for _ in
             range(DOCS_PER_BATCH)], ordered=False))

    @task(weight=1)
    def do_insert_clearing_bulk(self):
        self._process('insert-clearing-in-bulk', lambda: self.clearing_collection.insert_many(
            [self.generate_new_clearing() for _ in
             range(DOCS_PER_BATCH)], ordered=False))
