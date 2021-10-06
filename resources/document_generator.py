from typing import OrderedDict
from faker import Faker

class Authorisation:
    

    def generate_new_weighted_authorisation_doc():
        faker = Faker()

        """
        Generate an authorisation document using weights to switch between the 3 groups of merchants
        """
        document = {
            'auth_id': faker.uuid4(),
            'merch_id': faker.random_int(min=901000, max=901030),
            # using percentage weights to simulate the number of documents to insert for each group
            # Group 1 = 0.05%, Group 2 = 0.9% & Group 3 = 90.5%
            'merch_id': faker.random_element(elements=OrderedDict([
                    (faker.random_int(min=10000, max=10100), 0.005), # Group 1
                    (faker.random_int(min=20000, max=20020), 0.09), # Group 2
                    (faker.random_int(min=30001, max=30002), 0.905) # Group 3
                ])),
            'alliance_code': faker.lexify(text='????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
            'bank_name': faker.lexify(text='???? PLC.', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
            'bank_country': faker.country(),
            'posting_date': faker.date_time_between(start_date='-2y', end_date='-1y'),
            'merchant_details': {
                'external_merch_id': faker.random_int(min=100000, max=999999),
                'custom_merch_id': faker.random_int(min=100000, max=999999),
                'merch_type': faker.lexify(text='??????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'name': faker.company(),
                'trade_name': faker.company(),
                'msc_iso': faker.random_int(min=100000, max=1000000),
                'partner_code': faker.lexify(text='???', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ') + 
                str(faker.random_int(min=10000, max=20000))
            },
            'agent_code': faker.random_int(min=10000, max=1000000),
            'platform_code': faker.random_int(min=10000, max=1000000),
            'instrument_details': {
                'instrument_num': faker.random_int(min=10000, max=1000000),
                'instrument_num_masked': faker.random_int(min=1000000000000000, max=9000000000000000),
                'instrument_type_code': faker.lexify(text='???', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'instrument_type_subcode': faker.lexify(text='???', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
            },
            'auth_details': {
                'auth_code': faker.lexify(text='?????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'auth_date': faker.date_time_between(start_date='-2y', end_date='-1y'),
                'auth_exp': faker.date_time_between(start_date='-2y', end_date='-1y'),
                'auth_flag': 'AA',
                'auth_id': faker.bothify(text='####?##???#######?####?#', letters='abcdefghijklmnopqrstuvwxyz'),
                'auth_ref_id': faker.random_int(min=10000, max=1000000),
                'auth_seq': faker.random_int(min=1, max=10),
                'auth_time': faker.time(),
                'auth_method': faker.random_element(elements=OrderedDict([
                    ('ABC', 0.4),
                    ('123', 0.3),
                    ('XYZ', 0.3)
                ])),
                'auth_method_code': faker.random_int(min=1021, max=2131),
                'auth_platform_code': faker.random_int(min=1021, max=2131)
            },
            'acquirer_details': {
                'merch_id': faker.random_int(min=901000, max=901030),
                'resp_code': faker.random_int(min=9001, max=9999),
                'resp_code_desc': faker.sentence(nb_words=6, variable_nb_words=True),
                'terminal_id': faker.random_int(min=301000, max=301030),
                'acquired_id': faker.random_int(min=401000, max=401030),
                'bin': faker.lexify(text='????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'token': faker.bothify(text='####?##???#######?####?#', letters='abcdefghijklmnopqrstuvwxyz')
            },
            'account_details': {
                'account_type': faker.random_element(elements=OrderedDict([
                    ('Credit', 0.45),
                    ('Debit', 0.45),
                    ('Chequing', 0.05)
                ])),
                'account_type_code': faker.random_element(elements=OrderedDict([
                    ('CR1213', 0.45),
                    ('DB1212', 0.45),
                    ('CQ1213', 0.05)
                ])),
                'account_update_service_flag': faker.lexify(text='??', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')
            },
            'card_details': {
                'card_classification': faker.lexify(text='??????'),
                'card_curr_code': faker.random_element(elements=OrderedDict([
                    ('EUR', 0.45),
                    ('GBP', 0.45),
                    ('USD', 0.05)
                ])),
                'card_holder_name': faker.name(),
                'card_service_type_code': faker.lexify(text='????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'card_usage_code': faker.lexify(text='????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'card_verification': faker.lexify(text='??????'),
                'card_verification_code': faker.lexify(text='????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'cardholder_email': faker.free_email(),
                'card_present_indicator': faker.random_element(elements=OrderedDict([
                    ('N', 0.25),
                    ('Y', 0.75)
                ]))
            },
            'transaction_details': {
                'source_code': faker.lexify(text='?', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'location': faker.lexify(text='???', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'type': faker.lexify(text='??????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'transmission_date': faker.date_time_between(start_date='-2y', end_date='-1y'),
                'transmission_time': faker.time(),
                'merchant_tran_ref': faker.random_int(min=10000, max=1000000),
                'purchase_amount': faker.pyfloat(right_digits=2, positive=True, min_value=1, max_value=1000),
                'cashback_amount': faker.pyfloat(right_digits=2, min_value=0, max_value=10),
                'tip': faker.pybool(),
                'tip_amount': faker.pyfloat(right_digits=2, min_value=0, max_value=10)
            },
            'store_id': faker.random_int(min=901010, max=901020),
            'order_id': faker.random_int(min=901010, max=901020),
            'terminal_details': {
                'terminal_id': faker.random_int(min=1201010, max=3402020),
                'ip_address': faker.ipv4_public(),
                'terminal_curr_code': faker.random_element(elements=OrderedDict([
                    ('EUR', 0.45),
                    ('GBP', 0.45),
                    ('USD', 0.05)
                ])),
                'terminal_batch_num': faker.random_int(min=1201010, max=3402020),
                'pos_entry': faker.lexify(text='??', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
                'pos_capability': faker.lexify(text='???', letters='abcdefghijklmnopqrstuvwxyz')
            }
        }
        return document