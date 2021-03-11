import boto3
s3 = boto3.resource('s3',
    aws_access_key_id = '',
    aws_secret_access_key = '')

try:
    s3.create_bucket(Bucket='lwd7-bucket', CreateBucketConfiguration={
        'LocationConstraint': 'us-east-2'})
except:
 print ("this may already exist")

bucket = s3.Bucket("lwd7-bucket")

bucket.Acl().put(ACL='public-read')

body = open('exp1.csv', 'rb')

o = s3.Object('lwd7-bucket', 'test').put(Body=body )

s3.Object('lwd7-bucket', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb',
    region_name='us-east-2',
    aws_access_key_id='',
    aws_secret_access_key=''
)

try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("DataTable")

#wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)

import csv
import codecs
with open('experiments.csv', "rt", encoding="ANSI") as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        if(item[0] == 'partition'):
            continue
        body = open(item[4], 'rb')
        s3.Object('lwd7-bucket', item[4]).put(Body=body )
        md = s3.Object('lwd7-bucket', item[4]).Acl().put(ACL='public-read')

        url = " https://s3-us-east-2.amazonaws.com/lwd7-bucket/"+item[4]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
            'date' : item[2], 'comment' : item[3], 'url':url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

response = table.get_item(
    Key={
        'PartitionKey': 'experiment2',
        'RowKey': 'data2'
    }
)
item = response['Item']
print(item)

print(response)
