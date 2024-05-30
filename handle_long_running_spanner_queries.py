################################################################################################################
# Name: handle_long_running_spanner_queries.py
# Purpose: Identify long running Spanner queries based on the duration of the active sessions
#          and kill the sessions (Queries) that do not meet your threshold
# Developer: Lior (Leo) Ginzberg (liorginz@google.com)
# Date: 05/15/2024
# Version History:
# Version 1: 05/15 - Initial Version (Leveraging spanner_sys.oldest_active_queries)
#         2: 05/19 - Removed the use of spanner_sys.oldest_active_queries because the table does not have
#                    the instance and database information that is associated with every session in the table
# Routine Flow:
# 1. Define THRESHOLD_SECONDS - The definition in seconds for a long running query 
# 2. Start an indefinite WHILE loop that runs every 60 seconds (Configurable) 
# Do the following in every loop...
# 3. 
#    3.1 Get a list of all Spanner instances
#    3.2 For each instance, get a list of all the Spanner Databases
#    3.3 For each database, get a list of all sessions
#    3.4 Calculate the duration the session has been running (Compare creation time with current time)
#    3.5 If the calculated duration is greater than a threshold (COnfigurable) then kill the session (Query)
# CLI Execution: 
#    python handle_long_running_spanner_queries.py <project_id> *Replace <project_id> with your project_id
################################################################################################################
# Libraries
from google.cloud import spanner
from google.cloud import spanner_v1
from datetime import datetime, timezone
import time
import sys

# Threshold for considering a query "long-running" (in seconds)
THRESHOLD_SECONDS = 300  # 5 minutes

# Create Spanner clients
# Used to get session list
client = spanner_v1.SpannerClient()
# Initialized with project_id (sys.argv[1]
# Used to get instance list, instance details
spanner_client = spanner.Client(sys.argv[1])

if __name__ == "__main__":

    # Run the below logic continuously
    while True:

        running_time = datetime.now()
        print(f"Checking long running queries on: {running_time}")
        
        # Get list of instances
        instance_list = spanner_client.list_instances() 

        # Loop over each instance from the list
        for instance_id in instance_list:

            print(f"*Instance name: {instance_id.name}")

            # Extract the short instance name
            instance_name = instance_id.name[instance_id.name.rfind("/instances/")+11:]

            #print(f"Instance name: {instance_name}")
            # Get instance infromation
            instance = spanner_client.instance(instance_name)
            #print (f"instance: {instance}")
    
            # Get the list of all database per instance
            database_list = instance.list_databases()
    
            # Loop over each database from the list
            for database in database_list:
            
                # Extract the short database name
                database_name = database.name[database.name.rfind("/databases/")+11:]
                #WAS ONLY USED FOR TESTING
                #client.create_session(database=database.name)
                #time.sleep(5)
                print(f"****Database name: {database_name}")
                # Get the list of all active sessions for a given database
                session_list = client.list_sessions(database=database.name)
          
                # Loop over all session
                for session in session_list:

                    # Get the session short name
                    session_name = session.name[session.name.rfind("/sessions/")+10:]

                    print(f"      -Session Name: {session_name}")
                    # WAS ONLY USED FOR TESTING
                    #print(f"session create time: {session.create_time}")
                    #print(f"current time: {time.time()}")
                    #print(f"current date: {datetime.now(timezone.utc)}")
               
                    # Calculate how long, in seconds, the session has been running and preint it
                    session_running_time = datetime.now(timezone.utc) - session.create_time
                    print(f"       -session time in seconds: {session_running_time.total_seconds()}")

                    # Check is session run time is greater from the predefined THRESHOLD and if yes, delete it
                    if session_running_time.total_seconds() >= THRESHOLD_SECONDS:
                        client.delete_session(name=session.name)
                        print(f"       -Session {session_name} was killed after {session_running_time.total_seconds()} seconds")
        print("Waiting for the next execution...")
        time.sleep(60);

