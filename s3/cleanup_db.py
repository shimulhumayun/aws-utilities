import json
import datetime

# Create a log of file to be deleted
# save the list to s3 bucket
# return the list of dbs for cleanup
def create_log(session, params):
    glue = session.client('glue')
    s3 = session.client('s3')
    # get list of tables
    db_list = []
    for database in params['schemas']:
        response = glue.get_tables(
            DatabaseName=database
        )
        for table in response["TableList"]:
            db_list.append({'table_name': table["Name"],
                            "db_name": table["DatabaseName"],
                            "location": table["StorageDescriptor"]["Location"][len("s3://"):]})
    logdate = datetime.datetime.today().strftime('%Y%m%d')
    s3.put_object(Body=json.dumps({str("deleted"): db_list}),
                  Bucket=params['logbucket'],
                  Key='cleanup/logs-{date}.json'.format(date=logdate))
    output = json.dumps({str("deleted"): db_list})

    return db_list


# start deleting
def start_cleanup(session, params):
    log = create_log(session, params)
    # delete s3 data
    s3 = session.resource('s3')
    glue = session.client('glue')
    for db in log:
        # split bucket name and prefix the remove the data
        bucket, prefix = db["location"].split("/", 1)
        # delete data
        deleteData = s3.Bucket(bucket).objects.filter(Prefix=prefix).delete()
        # delete glue table
        deleteTable = glue.delete_table(Name=db["table_name"], DatabaseName=db["db_name"])
        print(deleteData)
        print(deleteTable)

# Start deleting tables
