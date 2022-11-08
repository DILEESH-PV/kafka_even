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
    reverse=int(i.value.decode())
    data=(str(reverse)[::-1])
    sql="INSERT INTO `rev`(`reverse`) VALUES (%s)"
    data1=(reverse,)
    mycursor.execute(sql,data1)
    mydb.commit()
        #if num==
    print("reverse number added",data)