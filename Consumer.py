from flask import Flask, Response
from kafka import KafkaConsumer
import numpy as np

kafka = KafkaClient('localhost:9092')
consumer = KafkaConsumer('Bank_Transaction1', 'Bank_Transaction2',group_id='view' bootstrap_servers=['0.0.0.0:9092'])
#Continuously listen to the connection and print messages as recieved
app = Flask(__name__)


@app.route('/')
def index():
    # return a multipart response
    return Response(kafkastream(),
                    mimetype='multipart/x-mixed-replace; boundary=frame')
def kafkastream():
    while True:
    msg = consumer.get_message()
    print("One recored Updated")

    mydb = MySQLdb.connect(host='localhost',
                    user='root',
                    passwd='',
                    db='Banking')
    cursor = mydb.cursor()
 #Maintain Total Balence
    cursor.execute("SELECT SUM(T.Amount), T.Dummy_ID FROM Transaction T GROUP BY T.Dummy_ID")
 #Average_Amount
 	cursor.execute("SELECT AVG(T.Amount), T.Dummy_ID FROM Transaction T GROUP BY T.Dummy_ID")
#Standard_Deviation
	cursor.execute("SELECT STDEV(T.Amount), T.Dummy_ID FROM Transaction T GROUP BY T.Dummy_ID")
#Monthly_Average_Transactional
	cursor.execute("SELECT year(date), month(date), Avg(T.Amount) FROM Transaction T GROUP BY  year(date), month(date)")

	



    if msg:
        print(msg)
    else:
        print('no new messages')




    
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)

kafka.close()

'''
for msg in consumer:
        
        yield (b'--frame\r\n'
               b'Content-Type: image/png\r\n\r\n' + msg.Transaction + b'\r\n\r\n')
        
        np.sum(msg.Amount)
        np.mean(msg.Amount)
        np.std(msg.Amount)
 '''