# Spanner---long-Running-Query
Identify long running Spanner queries, based on the duration of the active sessions and kill the relevant sessions (Queries) if they do not meet your threshold criteria

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
#    3.5 If the calculated duration is greater than a threshold (Configurable) then kill the session (Query)
# CLI Execution: 
#    python handle_long_running_spanner_queries.py <project_id> *Replace <project_id> with your project_id
################################################################################################################
