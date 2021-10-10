#!/usr/bin/env python3
import os, sys, math, datetime
from multiprocessing import Process
from pymongo import MongoClient
# import datetime
from load_settings import DB_NAME, CLUSTER_URL
import generate_authorisation

AUTH_COL = "authorisations"

# Global variables used in Main and Run functions
totalNumberOfProcesses = 0
processesList = []
totalDocs = 0
DOCS_PER_BATCH = 1000

def run(group, process_id, docs_to_insert, cluster_url):
    client = MongoClient(cluster_url)
    db = client[DB_NAME]
    coll = db[AUTH_COL]
    if (group == 1):
        min_group = 10000
        max_group = 10100
    elif (group == 2):
        min_group = 20000
        max_group = 20020
    else:
        min_group = 30001
        max_group = 30002
    
    # execute the insertion of the documents
    # for i in range(docs_to_insert):        
    #     doc = generate_authorisation.Authorisation.generate_new_grouped_authorisation_doc(min_group, max_group)
    #     coll.insert_one(doc)
    #     i += 1
    batches_to_process = 1
    if (docs_to_insert > DOCS_PER_BATCH):
        batches_to_process = math.ceil(docs_to_insert/DOCS_PER_BATCH)
    # execute the insertion of the documents
    for i in range(batches_to_process):        
        coll.insert_many(
            [generate_authorisation.Authorisation.generate_new_grouped_authorisation_doc(min_group, max_group) for _ in
             range(DOCS_PER_BATCH)], ordered=False)
        i += 1

if __name__ == '__main__':
    if len(sys.argv) > 3:
        totalDocs = int(sys.argv[1])
        group = int(sys.argv[2])
        totalNumberOfProcesses = int(sys.argv[3])
    else:
        print("-------------------------------------------------------------------------------------------------------")
        print("                                         USAGE ")
        print("-------------------------------------------------------------------------------------------------------")
        print("Please specify the number of documents to load and the group to load them into and the number of process to create \n e.g. `load_merchants.py 1000000 2 10` to load 1 million documents in merchant group 2 using 10 processes. \n")
        print("Merchants in group 1 will have an id between 10000 and 10100.")
        print("Merchants in group 2 will have an id between 20000 and 20020.")
        print("Merchants in group 3 will have an id of 30001 and 30002.\n")
        exit()

    uri = os.environ.get('MONGO_URI')
    print("got uri from env: ", uri)
    if not uri:
         uri = CLUSTER_URL

    print(uri)
    filename = "dataload.log"
    if os.path.exists(filename):
        os.remove(filename)

    # create the process list and add processes for each group
    for i in range(totalNumberOfProcesses):
        process = Process(target=run, args=(group, i, math.ceil(totalDocs / totalNumberOfProcesses), uri))
        processesList.append(process)

    # launch processes
    start_processing_time = datetime.datetime.now()
    start_processing_message = ("Launching " + str(totalNumberOfProcesses) +
                                " processes to insert " +
                                str(totalDocs) + 
                                " documents for merchant group "+str(group)+" at " + 
                                str(start_processing_time))
    print(start_processing_message)
    with open(filename, "a") as f:
        f.write(start_processing_message + "\n")
        
    for process in processesList:
        process.start()

    # wait for processes to complete
    for process in processesList:
        process.join()

    end_processing_time = datetime.datetime.now()
    processing_duration = end_processing_time - start_processing_time
    processing_duration_message = ("Processes completed at "+ str(end_processing_time) + 
                                    " and took " + str((processing_duration.total_seconds()) / 60) 
                                    + " minutes to complete.")
    processing_insert_rate_message = ("The combined insert rate was: " 
                            + str(round(totalDocs / processing_duration.total_seconds())) + 
                            " documents per second.")
    print(processing_duration_message)
    print(processing_insert_rate_message)
    with open(filename, "a") as f:
        f.write(processing_duration_message + "\n")
        f.write(processing_insert_rate_message + "\n")
