
import boto3,pdb
import psycopg2

# Create SQS client
sqs = boto3.client('sqs')

queue_url = 'https://us-west-2.queue.amazonaws.com/362759655961/sendQueue'
s_queue_url = 'https://sqs.us-west-2.amazonaws.com/362759655961/sendMessage.fifo'

try:
	conn = psycopg2.connect(database="postgres", user = "postgres", password = "htbl1234", host = "127.0.0.1", port = "5432")

	print "Opened database successfully"
	cur = conn.cursor()

	cur.execute("SELECT * from send")
	rows = cur.fetchall()

	for row in rows:
		did = str(row[0]) + str(row[4][0:5])
		did = did.replace(" ", "")
		print(did)

		if(row[4] == ''):
			continue
		response = sqs.send_message(
			QueueUrl=s_queue_url,
			MessageAttributes={
			'NameSendto': {
				'DataType': 'String',
				'StringValue': row[1]
			},
			'ProductName': {
				'DataType': 'String',
				'StringValue': row[3]
			},
			'PhoneNo': {
				'DataType': 'Number',
				'StringValue': row[0]
			}
			},
			MessageBody=(
				row[4]
			),

			MessageDeduplicationId = did,
			MessageGroupId = str(row[0])
		)
		if(len(response) > 0):
			print("message sent")	
		else:
			continue
		#pdb.set_trace()	

		if(row[5] != None):
			msghis = "'" + row[4] + "'," + str(row[5])
		else:
			msghis = "'" + row[4] + "'"
		statmt = "UPDATE send SET messagesend = '', messagehistory = %s WHERE phoneno = %s"
		cur.execute(statmt, (msghis,row[0]))
		conn.commit()
	conn.close()
except (Exception, psycopg2.DatabaseError) as error:
	print(error)
finally:
	if conn is not None:
		conn.close()
