import pandas as pd
from random import *
from faker import Faker
from kafka import SimpleProducer, KafkaClient
import csv
import MySQLdb

kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)
trans_index=0

topic = 'Bank_Transation'

def Transaction_Generation():

    mydb = MySQLdb.connect(host='localhost',
                    user='root',
                    passwd='',
                    db='Banking')
    cursor = mydb.cursor()

    csv_data = csv.reader(file('sample_data.csv'))
    for row in csv_data:

        cursor.execute('INSERT INTO Transaction(Dummy_ID, \
          Date_of_Txn, Transaction_Type, Amount )' \
          'VALUES("%s", "%s", "%s","%s")',row)
        producer.send_messages("row")
        trans_index=trans_index+1;



#close the connection to the database.
    cursor.close()
    print "Done"


kafka.close()

'''

#Random Data Generator

        data=pd.read_csv("sample_data.csv")

        Dummy_ID= RandomID()

        Date_of_Txn= RandDate()

        Transaction_Type= ['D','C]
                      
        Transaction= random.choice(Transaction_Type)

        Amount= rand.randint(1,100000)
                      
        data2= pd.DataFrame([Dummy_ID,Date_of_Txn,Transaction,Amount],columns=(['Dummy ID'],['Date of Txn'],['Transaction type'],['Amount']))

        data.append(data2,ignore_index=True)            

        producer.send_messages(topic,data)
            
        time.sleep(0.2)




def RandomIDigits():
    lower = 10**(7-1)
    upper = 10**7 - 1
    return random.randint(lower, upper)

def Randdate():
    fake = Faker()
    return fake.date_between(start_date='today', end_date='+30y')

'''