import boto3,pdb
import psycopg2
import threading
import multiprocessing
import time, csv, pdb

from multiprocessing import Process, Lock
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from webwhatsapi import WhatsAPIDriver
from webwhatsapi.objects.message import Message
from bs4 import BeautifulSoup
from geolocation.main import GoogleMaps
from csv import DictReader

# Create SQS client
sqs = boto3.client('sqs')
queue_url = 'https://us-west-2.queue.amazonaws.com/362759655961/sendQueue'
s_queue_url = 'https://sqs.us-west-2.amazonaws.com/362759655961/sendMessage.fifo'
r_queue_url = 'https://sqs.us-west-2.amazonaws.com/362759655961/recieveSQS.fifo'

conn = psycopg2.connect(database="postgres", user = "postgres", password = "htbl1234", host = "127.0.0.1", port = "5432")
print "Opened database successfully"
cur = conn.cursor()

google_maps = GoogleMaps(api_key='AIzaSyDNw4OLKRESwCrp1lLU6ojq9paZ85ZSqqs')  
driver = WhatsAPIDriver(client="chrome")
print("Waiting for QR")
driver.wait_for_login()
print("Bot started")


def send():
	msg = []
	#pdb.set_trace()
	try:
		while(True):
			#pdb.set_trace()
			response = sqs.receive_message(QueueUrl=s_queue_url,AttributeNames=['All'],MessageAttributeNames=['All'],MaxNumberOfMessages=1,VisibilityTimeout=10)['Messages']
			
			try:
				target = str(response[0]['MessageAttributes']['NameSendto']['StringValue'])
				strs = str(response[0]['Body'])

				search_xpath = '//*[@id="input-chatlist-search"]'
				driver.driver.find_element_by_xpath(search_xpath).send_keys(target + Keys.ENTER)
				
				new_value = '"'+target+'"'
				x_arg = '//span[contains(@title,' + new_value + ')]'
				driver.driver.find_element_by_xpath(x_arg).click()

				inp_xpath = '//*[@id="main"]/footer/div[1]/div[2]/div/div[2]'
				driver.driver.find_element_by_xpath(inp_xpath).send_keys(strs + Keys.ENTER)
				time.sleep(1)

				try:
					receipt_handle = response[0]['ReceiptHandle']
					delt = sqs.delete_message(QueueUrl=s_queue_url, ReceiptHandle=receipt_handle)
					print(delt)
				except:
					print("Message has not delete successfully but message send.\n")
			except:
				print("Message has not send successfully.\n")
	except:
		pass
	
	print("Send Thread Completed")


def sendtosqs(name, phone, gid, msg, addr, latlng):
	#pdb.set_trace()
	did = msg[0:5] + phone
	did = did.replace(" ", "")
	response = sqs.send_message(
		QueueUrl=r_queue_url,
		MessageAttributes={
		'NameRecievefrom': {
			'DataType': 'String',
			'StringValue': str(name)
		},
		'PhoneNo': {
			'DataType': 'String',
			'StringValue': str(phone)
		},
		'MsgType': {
			'DataType': 'String',
			'StringValue': str(gid)
		},
		'Location': {
			'DataType': 'String',
			'StringValue': str(latlng)
		},
		'Address': {
			'DataType': 'String',
			'StringValue': str(addr)
		}
		},
		MessageBody = str(msg),

		MessageDeduplicationId = did,
		MessageGroupId = gid
	)

def recieve():
	#pdb.set_trace()
	while True:
		time.sleep(3)
		for contact in driver.get_unread():
			for message in contact.messages:
				try:
					name = str(contact).split('in ')[1].split('>')[0]
					phone = str(contact.chat).split(': ')[1].split('@')[0]
					if 'MediaMessage' in str(message):
						if('image') in str(message):
							sendtosqs(name,phone,'MediaMessage','Image','NULL','NULL')
						else:
							sendtosqs(name,phone,'MediaMessage','Video','NULL','NULL')
						continue
					elif 'MMSMessage' in str(message):
						sendtosqs(name,phone,'MMSMessage','client shared a document with you','NULL','NULL')
						continue
					else:
						msgsplit = str(message).split(': ')[1].split('>')[0].encode('utf-8')

					search_xpath = '//*[@id="input-chatlist-search"]'
					driver.driver.find_element_by_xpath(search_xpath).send_keys(name + Keys.ENTER)
					time.sleep(2)
					html_doc = driver.driver.page_source
					soup = BeautifulSoup(html_doc, 'html.parser')

					flag = 0
					for item in soup.find_all('a'):
						if not item.find_all('img'):
							continue
						msg = item.find_all('img')[0].get('style').split(',')[1].split('"')[0].encode('utf-8')
						if msg == msgsplit:
							href = item.get('href')
							#pdb.set_trace()
							flag = 1
							lat = href.split('?q=')[1].split('%')[0]
							lng = href.split('%2C')[1].split('&')[0]
							latlng = str(lat) + ',' + str(lng)
							try:
								location = google_maps.search(lat=lat, lng=lng).first()
								print(location.formatted_address)
								locate = location.formatted_address.encode('utf-8')
								break
							except:
								flag = 2
								print("Over your quota.")

					if(flag == 0):
						sendtosqs(name,phone,'TEXT', msgsplit.encode('utf-8'),'NULL','NULL')

					elif(flag == 1):
						sendtosqs(name,phone,'Address','Address',locate, latlng)
					else:
						sendtosqs(name,phone,'Address','Over your quota','NULL', latlng)
				except:
					print("Error\n")
	print("Recieve Thread Completed")


if __name__ == "__main__":
	send()
	# t2 = multiprocessing.Process(target=recieve)

	# flag = 0
	# while (True):
	# 	send()
	# 	if(flag == 0):
	# 		t2.start()
	# 		flag = 1
	# 	t2.join(150)
	# 	print("Recieve Thread Completed")
	# print("Done!")