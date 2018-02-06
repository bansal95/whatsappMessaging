import boto3,pdb
import pymongo
from pymongo import MongoClient
# Create SQS client
sqs = boto3.client('sqs')

r_queue_url = 'https://sqs.us-west-2.amazonaws.com/362759655961/recieveSQS.fifo'

# Receive message from SQS queue

try:
    m_client = MongoClient('mongodb://localhost:27017/',connect=False)
    db = m_client['whatsapp']
    collection = db['receive']
    
    try:
        while(True):
            response = sqs.receive_message(
                QueueUrl=r_queue_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                MessageAttributeNames=['All'],
                VisibilityTimeout=10
            )

            #pdb.set_trace()
            name = response['Messages'][0]['MessageAttributes']['NameRecievefrom']['StringValue']
            phone = response['Messages'][0]['MessageAttributes']['PhoneNo']['StringValue']
            mesgtype = response['Messages'][0]['MessageAttributes']['MsgType']['StringValue']
            message = response['Messages'][0]['Body']
            address = response['Messages'][0]['MessageAttributes']['Address']['StringValue']
            latlng = response['Messages'][0]['MessageAttributes']['Location']['StringValue']

            
            try:    
                cursor = collection.find({"PhoneNo" : phone})
                message = "'" + message + "'"
                try:
                    if(cursor[0] > 0):
                        msg = cursor[0]["Messages"] + ', ' + message
                        if(latlng != 'NULL'):
                            post = {"Messages": msg,  "Address" : address, "LatLong" : latlng}
                            collection.update_one({"PhoneNo": phone},{"$set": post})
                        else:
                            post = {"Messages": msg}
                            collection.update_one({"PhoneNo": phone},{"$set": post})
                except:
                    post = {"Name" : name, "PhoneNo" : phone, "Messages" : message, "Address" : address, "LatLong" : latlng}
                    collection.insert(post)
            except:
                print("MongoDB Updation Error\n")
                continue

            rech = response['Messages'][0]['ReceiptHandle']
            response = sqs.delete_message(ReceiptHandle = rech, QueueUrl= r_queue_url)
            print(response)
    except:
        pass
except:
    print(error)
    