#!/usr/bin/env python3
import os, sys
sys.path.append('resources/')
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from multiprocessing import Process
from pymongo import MongoClient
import datetime
from settings import DB_NAME, CLUSTER_URL, PROCESSES
import document_generator

AUTH_COL = "authorisations"

# Global variables used by the Main and Run functions
processesList = []
totalDocs = 1613300000

def run(process_id, docs_to_insert, logFileName):
    print("Connecting to Atlas Cluster at: " + CLUSTER_URL)
    client = MongoClient(CLUSTER_URL)
    db = client[DB_NAME]
    coll = db[AUTH_COL]

    # log the beginning of processing
    start_time = datetime.datetime.now()
    start_message = "Process: " + str(process_id) + " started inserting " + str(docs_to_insert) + " documents at " + str(start_time)
    
    with open(logFileName, "a") as f:
        f.write(start_message + "\n")
    
    print(start_message)

    # execute the insertion of the documents
    for i in range(docs_to_insert):
        doc = document_generator.Authorisation.generate_new_weighted_authorisation_doc()
        result = coll.insert_one(doc)
        i += 1

        printStr = (str(datetime.datetime.now()) + 
                    ": Process: " + 
                    str(process_id) + 
                    " generated authorisation document with auth_id: " + 
                    str(doc["auth_id"]) + 
                    " and merchant ID: " + 
                    str(doc["merch_id"]) + "\n")
        with open(logFileName, "a") as f:
            f.write(printStr)

    end_time = datetime.datetime.now()
    end_message = ("Process: " + 
                    str(process_id) + 
                    " completed inserting " + 
                    str(docs_to_insert) + 
                    " documents at " + str(end_time))
    duration = end_time - start_time
    duration_message = ("Process: " + 
                        str(process_id) + 
                        " took " + 
                        str((duration.total_seconds()) / 60) + 
                        " minutes to complete.")
    insert_rate_message = ("The insert rate was: " 
                            + str(round(docs_to_insert / duration.total_seconds())) + 
                            " documents per second.")

    # log results
    with open(logFileName, "a") as f:
        f.write(end_message + "\n")
        f.write(duration_message + "\n")
        f.write(insert_rate_message + "\n")

    print(end_message)
    print(duration_message)
    print(insert_rate_message)

if __name__ == '__main__':
    print("len(sys.argv) = " + str(len(sys.argv)))
    if len(sys.argv) > 1:
        totalDocs = int(sys.argv[1])

    if (totalDocs <= PROCESSES):
        PROCESSES = totalDocs

    filename = "runlogs/Merchants_Inserts_Log_" + str(totalDocs) + ".log"
    print(filename)
    if os.path.exists(filename):
        os.remove(filename)
    
    printStr = ("Launching " + str(PROCESSES) + 
                " processes to insert " +
                str(totalDocs) +
                " documents...\n")
    
    with open(filename, "a") as f:
        f.write(printStr)

    print(printStr)

    # create the process list
    for i in range(PROCESSES):
        process = Process(target=run, args=(i, round(totalDocs / PROCESSES), filename))
        processesList.append(process)

    # launch processes
    start_processing_time = datetime.datetime.now()
    start_processing_message = (str(PROCESSES) +
                                " processes started processing " +
                                str(totalDocs) + 
                                " documents at " + 
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
    end_processing_message = (str(PROCESSES) + 
                " processes completed at " +
                str(end_processing_time))
    print(end_processing_message)
    with open(filename, "a") as f:
        f.write(end_processing_message + "\n")

    processing_duration = end_processing_time - start_processing_time
    processing_duration_message = ("Processes took " + 
                        str((processing_duration.total_seconds()) / 60) + 
                        " minutes to complete.")
    processing_insert_rate_message = ("The combined insert rate was: " 
                            + str(round(totalDocs / processing_duration.total_seconds())) + 
                            " documents per second.")

    # log full results
    with open(filename, "a") as f:
        f.write(processing_duration_message + "\n")
        f.write(processing_insert_rate_message + "\n\n")

    with open(filename, "r") as f:
        print(f.read())