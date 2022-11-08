from kafka import KafkaConsumer
import mysql.connector
try:
    mydb=mysql.connector.connect(host='localhost',user='root',password='',database='numberdb')
except mysql.connector.Error as e:
    print(e)
mycursor=mydb.cursor()

bootstrap_server = ["localhost:9092"]

topic = "num"

consumer = KafkaConsumer(topic, bootstrap_servers = bootstrap_server)

for i in consumer:
    print(str(i.value.decode()))
    numbers=int(i.value.decode())
    if (numbers%2)==0:
        sql="INSERT INTO `num`(`evenvalue`) VALUES ('"+str(numbers)+"')"
        #data=(numbers,)
        mycursor.execute(sql)
        mydb.commit()
        print("even number added",numbers)