from locust import task, between
from mongo_user import MongoUser
import generate_authorisation

# docs to insert per batch insert
DOCS_PER_BATCH = 1000
min_group = 30001
max_group = 30002


class MongoSampleUser(MongoUser):
    """
    Generic sample mongodb workload generator
    """
    # no delays between operations
    wait_time = between(0.0, 0.0)

    def __init__(self, environment):
        super().__init__(environment)
        self.authorisation_collection = None

    def on_start(self):
        """
        Executed every time a new test is started - place init code here
        """
        # prepare the collection
        # index1 = pymongo.IndexModel([('auth_id', pymongo.ASCENDING),('alliance_code', pymongo.ASCENDING),("posting_date", pymongo.DESCENDING)],
        #                             name="auth_id_compound")
        self.authorisation_collection = self.ensure_collection('authorisations')

    # @task(weight=1)
    # def do_insert_document(self):
    #     self._process('insert-document', self.insert_single_document)

    @task(weight=1)
    def do_insert_auth_bulk(self):
        self._process('insert-authorisations-in-bulk', lambda: self.authorisation_collection.insert_many(
            [generate_authorisation.Authorisation.generate_new_grouped_authorisation_doc(min_group, max_group) for _ in
             range(DOCS_PER_BATCH)], ordered=False), DOCS_PER_BATCH)
