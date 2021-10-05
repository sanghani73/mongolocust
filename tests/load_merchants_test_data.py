from typing import OrderedDict
from locust import task, between
from mongo_user import MongoUser

import pymongo

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

    def generate_new_authorisation(self):
        """
        Generate an authorisation document using weights to switch between the 3 groups of merchants
        """
        document = {
            'auth_id': self.faker.uuid4(),
            'merch_id': self.faker.random_int(min=901000, max=901030),
            'merch_id': self.faker.random_element(elements=OrderedDict([
                    (self.faker.random_int(min=10000, max=10100), 0.0001), # Group 1
                    (self.faker.random_int(min=20000, max=20020), 0.0009), # Group 2
                    (self.faker.random_int(min=30001, max=30002), 0.999) # Group 3
                ])),
            'alliance_code': self.faker.lexify(text='????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
            'bank_name': self.faker.lexify(text='???? PLC.', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
            'bank_country': self.faker.country(),
            'posting_date': self.faker.date_time_between(start_date='-2y', end_date='-1y'),
            'merchant_details': {
                'external_merch_id': self.faker.random_int(min=100000, max=999999),
                'custom_merch_id': self.faker.random_int(min=100000, max=999999),
                'merch_type': self.faker.lexify(text='??????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'name': self.faker.company(),
                'trade_name': self.faker.company(),
                'msc_iso': self.faker.random_int(min=100000, max=1000000),
                'partner_code': self.faker.lexify(text='???', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ') + 
                str(self.faker.random_int(min=10000, max=20000))
            },
            'agent_code': self.faker.random_int(min=10000, max=1000000),
            'platform_code': self.faker.random_int(min=10000, max=1000000),
            'instrument_details': {
                'instrument_num': self.faker.random_int(min=10000, max=1000000),
                'instrument_num_masked': self.faker.random_int(min=1000000000000000, max=9000000000000000),
                'instrument_type_code': self.faker.lexify(text='???', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'instrument_type_subcode': self.faker.lexify(text='???', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
            },
            'auth_details': {
                'auth_code': self.faker.lexify(text='?????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'auth_date': self.faker.date_time_between(start_date='-2y', end_date='-1y'),
                'auth_exp': self.faker.date_time_between(start_date='-2y', end_date='-1y'),
                'auth_flag': 'AA',
                'auth_id': self.faker.bothify(text='####?##???#######?####?#', letters='abcdefghijklmnopqrstuvwxyz'),
                'auth_ref_id': self.faker.random_int(min=10000, max=1000000),
                'auth_seq': self.faker.random_int(min=1, max=10),
                'auth_time': self.faker.time(),
                'auth_method': self.faker.random_element(elements=OrderedDict([
                    ('ABC', 0.4),
                    ('123', 0.3),
                    ('XYZ', 0.3)
                ])),
                'auth_method_code': self.faker.random_int(min=1021, max=2131),
                'auth_platform_code': self.faker.random_int(min=1021, max=2131)
            },
            'acquirer_details': {
                'merch_id': self.faker.random_int(min=901000, max=901030),
                'resp_code': self.faker.random_int(min=9001, max=9999),
                'resp_code_desc': self.faker.sentence(nb_words=6, variable_nb_words=True),
                'terminal_id': self.faker.random_int(min=301000, max=301030),
                'acquired_id': self.faker.random_int(min=401000, max=401030),
                'bin': self.faker.lexify(text='????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'token': self.faker.bothify(text='####?##???#######?####?#', letters='abcdefghijklmnopqrstuvwxyz')
            },
            'account_details': {
                'account_type': self.faker.random_element(elements=OrderedDict([
                    ('Credit', 0.45),
                    ('Debit', 0.45),
                    ('Chequing', 0.05)
                ])),
                'account_type_code': self.faker.random_element(elements=OrderedDict([
                    ('CR1213', 0.45),
                    ('DB1212', 0.45),
                    ('CQ1213', 0.05)
                ])),
                'account_update_service_flag': self.faker.lexify(text='??', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
            },
            'card_details': {
                'card_classification': self.faker.lexify(text='??????'),
                'card_curr_code': self.faker.random_element(elements=OrderedDict([
                    ('EUR', 0.45),
                    ('GBP', 0.45),
                    ('USD', 0.05)
                ])),
                'card_holder_name': self.faker.name(),
                'card_service_type_code': self.faker.lexify(text='????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'card_usage_code': self.faker.lexify(text='????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'card_verification': self.faker.lexify(text='??????'),
                'card_verification_code': self.faker.lexify(text='????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'cardholder_email': self.faker.free_email(),
                'card_present_indicator': self.faker.random_element(elements=OrderedDict([
                    ('N', 0.25),
                    ('Y', 0.75)
                ]))
            },
            'transaction_details': {
                'source_code': self.faker.lexify(text='?', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'location': self.faker.lexify(text='???', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'type': self.faker.lexify(text='??????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'transmission_date': self.faker.date_time_between(start_date='-2y', end_date='-1y'),
                'transmission_time': self.faker.time(),
                'merchant_tran_ref': self.faker.random_int(min=10000, max=1000000),
                'purchase_amount': self.faker.pyfloat(right_digits=2, positive=True, min_value=1, max_value=1000),
                'cashback_amount': self.faker.pyfloat(right_digits=2, min_value=0, max_value=10),
                'tip': self.faker.pybool(),
                'tip_amount': self.faker.pyfloat(right_digits=2, min_value=0, max_value=10)
            },
            'store_id': self.faker.random_int(min=901010, max=901020),
            'order_id': self.faker.random_int(min=901010, max=901020),
            'terminal_details': {
                'terminal_id': self.faker.random_int(min=1201010, max=3402020),
                'ip_address': self.faker.ipv4_public(),
                'terminal_curr_code': self.faker.random_element(elements=OrderedDict([
                    ('EUR', 0.45),
                    ('GBP', 0.45),
                    ('USD', 0.05)
                ])),
                'terminal_batch_num': self.faker.random_int(min=1201010, max=3402020),
                'pos_entry': self.faker.lexify(text='??', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'pos_capability': self.faker.lexify(text='???', letters='abcdefghijklmnopqrstuvwxyz')
            }
        }
        return document

    def on_start(self):
        """
        Executed every time a new test is started - place init code here
        """
        # prepare the collection
        index1 = pymongo.IndexModel([('auth_id', pymongo.ASCENDING),('alliance_code', pymongo.ASCENDING),("posting_date", pymongo.DESCENDING)],
                                    name="auth_id_compound")
        self.authorisation_collection = self.ensure_collection('authorisations', index1)

    # @task(weight=1)
    # def do_insert_document(self):
    #     self._process('insert-document', self.insert_single_document)

    @task(weight=1)
    def do_insert_auth_bulk(self):
        self._process('insert-authorisations-in-bulk', lambda: self.authorisation_collection.insert_many(
            [self.generate_new_authorisation() for _ in
             range(DOCS_PER_BATCH)], ordered=False), DOCS_PER_BATCH)