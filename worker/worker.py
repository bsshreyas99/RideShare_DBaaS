import time
import pika
import os
import mysql.connector
from kazoo.client import KazooClient
import uuid

while(not os.path.exists("/app/mypid.txt") or not os.path.exists("/app/replica.txt")):
    pass

pid_file = open("/app/mypid.txt","r")
pid = int(pid_file.readline())
pid_file.close()

rabbitmq_ip = os.environ["RABBITMQ_IP"]
mysql_db = os.environ["MYSQL_IP_ADDR"]

mydb = mysql.connector.connect(
    host = mysql_db,
    user="root",
    password="root",
    port=3306,
    database="cloud_ass3")

mycursor = mydb.cursor()
channel = None

for line in open("/app/replica.txt"):
    if(line):
        mycursor.execute(line)
        
mydb.commit()

zk = KazooClient(hosts="172.17.0.3:2181")
zk.start()

def func(event):
    
    global channel
    new_node2 = new_node.split("/")[2]
    if(new_node2 == min(zk.get_children("/election"),key = lambda x:x.split("_")[1])):
        leader_function()

    else:
        to_watch = "_-1"
        seq = new_node.split("_")[1]
        
        for i in zk.get_children("/election"):
            if(int(i.split("_")[1]) < int(seq) and int(i.split("_")[1]) > int(to_watch.split("_")[1])):
                to_watch = i

        zk.exists("/election"+"/"+to_watch,watch=func)

    


def leader_function():
    global channel

    file = open("leader.txt",'a')
    file.write("I am leader ")
    file.close()

    
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbitmq_ip,heartbeat=0))
    channel = connection.channel()

    channel.queue_declare(queue='writeq', durable=True)    
        
    #declare syncQ
    channel.exchange_declare(exchange='sync', exchange_type='fanout')
    
    def callback(ch, method, properties, body):
        file = open("test.txt",'a')
        body = body.decode('utf-8')
        #send sql query to sync Q
        channel.basic_publish(exchange='sync', routing_key='', body=body)
        file.write("Received "+body+'\n')
        try:
            mycursor.execute(body)
            mydb.commit()
        except:
            pass
        file.close()
            
    channel.basic_consume(queue="writeq", on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
        
def slave_function():
    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=rabbitmq_ip,heartbeat=0))
    channel = connection.channel()
    channel.queue_declare(queue='readq', durable=True)
    channel.queue_declare(queue='responseq', durable=True)
    
    def callback(ch, method, properties, body):
        file = open("test.txt",'a')
        body = body.decode('utf-8')
        file.write("Received "+body+'\n')
        ch.basic_ack(delivery_tag = method.delivery_tag)
        file.close()
        res = []
        mycursor.execute(body)
        
        for i in mycursor.fetchall():
            res.append(i)
            
        channel.basic_publish(exchange='', routing_key='responseq', body=str(res))
        
    def callback2(ch, method, properties, body):
        file = open("test_sync.txt",'a')
        body = body.decode('utf-8')
        try:
            mycursor.execute(body)
            mydb.commit()
        except:
            pass        
        file.write("Received "+body+'\n')
        file.close()

    channel.exchange_declare(exchange='sync', exchange_type='fanout')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange='sync', queue=queue_name)
    
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="readq", on_message_callback=callback)
    channel.basic_consume(
        queue=queue_name, on_message_callback=callback2, auto_ack=True)
    channel.start_consuming()

if(len(zk.get_children("/election"))==0):
    unique_id = uuid.uuid4().hex+"_"+str(pid)
    new_node = zk.create("/election"+"/"+unique_id,ephemeral=True)
    leader_function()
        
else:
    unique_id = uuid.uuid4().hex+"_"+str(pid)
    new_node = zk.create("/election"+"/"+unique_id,ephemeral=True)
    to_watch = "_-1"
    seq = new_node.split("_")[1]
    for i in zk.get_children("/election"):
        if(int(i.split("_")[1]) < int(seq) and int(i.split("_")[1]) > int(to_watch.split("_")[1])):
            to_watch = i
    print("Watching "+to_watch)
    zk.exists("/election"+"/"+to_watch,watch=func)
    slave_function()
