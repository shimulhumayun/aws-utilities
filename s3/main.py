import boto3
import cleanup_db
import recover_s3data

backup = {
    'database': 'backupdb',
    'bucket': 'aws-devops-course-shimul',
    'logbucket': 'aws-devops-course-shimul',
    'path': '/output',
    'prefix': "sqa",
    'schemas': ["imdbdata", "sampledb"],
    'query': ("CREATE TABLE backupdb.cmpcts"
              "WITH ("
              "external_location  = 's3://aws-devops-course-shimul/testshimul/')"
              "AS SELECT *"
              "FROM sampledb.superstore ;")
}

restore = {
    'logbucket': 'aws-devops-course-shimul',
    'logFile': 'cleanup/logs-20211107.json',
    'schemas': ['list of glue database to restore'],
    'prefix': 'database prefix. Ex: sqa1, sqa1 or sqa2'
}

session = boto3.Session()

# Function for obtaining query results and location
# location, data = athena_from_s3.query_results(session, params)
# backup_table.backup(session, params)
'''print("Locations: ", location)
print("Result Data: ")
print(data)'''
# cleanup_db.create_log(session, params)
# cleanup_db.start_cleanup(session, backup)
recover_s3data.read_file(session, restore)
# Function for cleaning up the query results to avoid redundant data
# S3_cleanup.clean_up()create_log
