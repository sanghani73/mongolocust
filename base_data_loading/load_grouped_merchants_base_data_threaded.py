#!/usr/bin/env python3
import os, sys
sys.path.append('resources/')
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
from multiprocessing import Process
from pymongo import MongoClient
import datetime
from settings import DB_NAME, CLUSTER_URL, GROUP_1_PROCESSES, GROUP_2_PROCESSES, GROUP_3_PROCESSES
import document_generator

AUTH_COL = "authorisations"

# Global variables used in Main and Run functions
totalNumberOfProcesses = 0
processesList = []
totalDocs = 0

def run(group, process_id, docs_to_insert, logFileName):
    print("Group 1 connecting to Atlas Cluster at: " + CLUSTER_URL)
    client = MongoClient(CLUSTER_URL)
    db = client[DB_NAME]
    coll = db[AUTH_COL]

    # log the beginning of processing
    start_time = datetime.datetime.now()
    start_message = group + " - Process: " + str(process_id) + " started inserting " + str(docs_to_insert) + " documents at " + str(start_time)
    
    with open(logFileName, "a") as f:
        f.write(start_message + "\n")
    
    print(start_message)

    #generate min and max group with defaults for group 1
    min_group = 10000
    max_group = 10100 

    if(group == "Group 2"):
        min_group = 20000
        max_group = 20020
    if(group == "Group 3"):
        min_group = 30001
        max_group = 30002

    # execute the insertion of the documents
    for i in range(docs_to_insert):        
        doc = document_generator.Authorisation.generate_new_grouped_authorisation_doc(min_group, max_group)

        coll.insert_one(doc)
        i += 1

        printStr = (str(datetime.datetime.now()) + 
                    ": " + group + " Group 1 - Process: " + 
                    str(process_id) + 
                    " generated authorisation document with auth_id: " + 
                    str(doc["auth_id"]) + 
                    " and merchant ID: " + 
                    str(doc["merch_id"]) + "\n")
        with open(logFileName, "a") as f:
            f.write(printStr)

    end_time = datetime.datetime.now()
    end_message = (group + " - Process: " + 
                    str(process_id) + 
                    " completed inserting " + 
                    str(docs_to_insert) + 
                    " documents at " + str(end_time))
    duration = end_time - start_time
    duration_message = (group + " - Process: " + 
                        str(process_id) + 
                        " took " + 
                        str((duration.total_seconds()) / 60) + 
                        " minutes to complete.")
    insert_rate_message = (group + " - The insert rate was: " 
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
    if len(sys.argv) > 3:
        groupOneDocs = int(sys.argv[1])
        groupTwoDocs = int(sys.argv[2])
        groupThreeDocs = int(sys.argv[3])
        totalDocs = groupOneDocs + groupTwoDocs + groupThreeDocs
    else:
        print("Please specify the number of documents to load for each group. This script expects 3 number of documents to be provided as arguments.")
        exit()

    # for i in range(2):
    #     if i == 0:
    #         if (groupOneDocs <= GROUP_1_PROCESSES):
    #             GROUP_1_PROCESSES = groupOneDocs
    #     if i == 1:
    #         if (groupTwoDocs <= GROUP_2_PROCESSES):
    #             GROUP_2_PROCESSES = groupTwoDocs
    #     if i == 2:
    #         if (groupThreeDocs <= GROUP_3_PROCESSES):
    #             GROUP_3_PROCESSES = groupThreeDocs

    filename = "runlogs/Merchants_Inserts_Log_" + str(totalDocs) + ".log"
    print(filename)
    if os.path.exists(filename):
        os.remove(filename)
        
    totalNumberOfProcesses = GROUP_1_PROCESSES + GROUP_2_PROCESSES + GROUP_3_PROCESSES
    printStr = ("Launching " + str(totalNumberOfProcesses) + 
                " processes to insert " +
                str(totalDocs) +
                " documents...\n")
        
    with open(filename, "a") as f:
        f.write(printStr)

    print(printStr)

    # create the process list and add processes for each group
    for i in range(GROUP_1_PROCESSES):
        process = Process(target=run, args=("Group 1", i, round(groupOneDocs / GROUP_1_PROCESSES), filename))
        processesList.append(process)

    for i in range(GROUP_2_PROCESSES):
        process = Process(target=run, args=("Group 2", i, round(groupTwoDocs / GROUP_2_PROCESSES), filename))
        processesList.append(process)

    for i in range(GROUP_3_PROCESSES):
        process = Process(target=run, args=("Group 3", i, round(groupThreeDocs / GROUP_3_PROCESSES), filename))
        processesList.append(process)

    # launch processes
    start_processing_time = datetime.datetime.now()
    start_processing_message = (str(totalNumberOfProcesses) +
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
    end_processing_message = (str(totalNumberOfProcesses) + 
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