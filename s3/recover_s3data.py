import boto3
import json


# read deleted content
def read_file(session, param):
    s3 = session.resource('s3')
    content_object = s3.Object(param["logbucket"], param['logFile'])
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content["deleted"]


def restore(session, param):
    paginator = session.get_paginator('list_object_versions')
    #thebucket = "aws-devops-course-shimul"
    s3 = boto3.resource('s3')
    for db in read_file(session, param):
        bucketname, prefix = db["location"].split("/", 1)
        pageresponse = paginator.paginate(Bucket=bucketname)
        # s3 = boto3.resource('s3')
        for pageobject in pageresponse:
            if 'DeleteMarkers' in pageobject.keys():
                for each_delmarker in pageobject['DeleteMarkers']:
                    fileobjver = s3.ObjectVersion(
                        bucketname,
                        each_delmarker['Key'],
                        each_delmarker['VersionId']
                    )
                    if each_delmarker["IsLatest"] and each_delmarker["Key"].startswith(prefix):
                        fileobjver.delete()


'''if __name__ == "__main__":
    restore()'''
