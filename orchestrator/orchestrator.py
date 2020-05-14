import docker
import pika
from kazoo.client import KazooClient
from flask import *
import time
import uuid
import os
import threading
import datetime

app = Flask(__name__)
return_str = ""

#Variable used to count read requests
read_req=0


#--------------------------------------------- APIs ------------------------------------------------------

#Clear database
@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
    sql_query = "DELETE FROM users"
    
    replica_file = open("replica.txt","a")
    replica_file.write(sql_query+"\n")
    replica_file.close()
    
    #Publish query into writeQ
    channel.basic_publish(exchange='', routing_key='writeq', body=sql_query)
    return 'Cleared database',200


#Delete specific rows in the database
@app.route('/api/v1/db/delop',methods=["POST"])
def del_db():
    sql_query = request.get_json()['query']
    
    replica_file = open("replica.txt","a")
    replica_file.write(sql_query+"\n")
    replica_file.close()
    
    #Publish query into writeQ
    channel.basic_publish(exchange='', routing_key='writeq', body=sql_query)

    return '',201


#Write to the database
@app.route('/api/v1/db/write',methods=["POST"])
def write_db():
    to_ins = request.get_json()['insert'] # [ 'shashank', '6ae999552a0d2dca14d62e2bc8b764d377b1dd6c' ]
    to_col = request.get_json()['column'] #  [ 'username' , 'password' ]
    to_table = request.get_json()['table'] # users

    #Construct sql query
    sql_query = "INSERT INTO "+to_table+" "+str(to_col).replace('[','(').replace(']',')').replace("'","")+" values "+str(to_ins).replace('[','(').replace(']',');')

    #Write query into replica file
    replica_file = open("replica.txt","a")
    replica_file.write(sql_query+"\n")
    replica_file.close()
    
    #Publish query into writeQ
    channel.basic_publish(exchange='', routing_key='writeq', body=sql_query)

    return '',201


#Read from the database
@app.route('/api/v1/db/read',methods=["POST"])
def read_db():
    global read_req
    to_col = request.get_json()['column']
    to_table = request.get_json()['table']
    to_conds = request.get_json()['where']

    #Construct sql query
    sql_query = "SELECT " + str(to_col).replace('[',"").replace("]","").replace("'","") + " "
    sql_query += "FROM " + str(to_table) + " "
    if(len(to_conds)==1):
        sql_query += "WHERE "+to_conds[0]
    else:
        sql_query += "WHERE " + str(to_conds).replace('[',"").replace("]","").replace("\"","").replace(","," and ") + ";"
    print(sql_query)
    
    #Increment number of read requests
    read_req+=1
    
    #Publish query into readQ
    channel.basic_publish(exchange='',
                      routing_key='readq',
                      body=sql_query,
                      properties=pika.BasicProperties(delivery_mode=2))


    def callback(ch,method,properties,body):
        global return_str
        return_str = body.decode('utf-8')
        channel.stop_consuming()

	#Consume from responseQ    
    channel.basic_consume(queue="responseq", on_message_callback=callback)
    channel.start_consuming()
    
    print(return_str)
    
    #Return result
    if(eval(return_str)):
        return str(return_str),200
    abort(400)


#Crash slave container
@app.route('/api/v1/crash/slave',methods=["POST"])
def crash_slave():
    to_ret = int(max([app_db_mapping[i][0].top()["Processes"][0][1] for i in app_db_mapping]))
    scale_in()
    return str([to_ret]),200


#Crash master container
@app.route('/api/v1/crash/master',methods=["POST"])
def crash_master():
    to_kill = min([app_db_mapping[i][0].top()["Processes"][0][1] for i in app_db_mapping])
    for i in app_db_mapping:
        if(to_kill == app_db_mapping[i][0].top()["Processes"][0][1]):
            app_db_mapping[i][0].stop()
            app_db_mapping[i][1].stop()
            app_db_mapping[i][0].remove()
            app_db_mapping[i][1].remove()
            temp = i
    del(app_db_mapping[temp])
    return str([int(to_kill)]),200


#List PIDs of all worker containers
@app.route('/api/v1/worker/list',methods=["GET"])
def list_worker():
    workers = []
    for i in app_db_mapping:
        workers.append(int(app_db_mapping[i][0].top()["Processes"][0][1]))
    return str(sorted(workers)),200



#Spawn a new worker
def respawn_slave():
    print("I am spawning a new worker")
    slave_db = client.containers.run("mysql_new:latest",environment=["MYSQL_ROOT_PASSWORD=root","MYSQL_DATABASE=cloud_ass3"],detach=True)
    slave_db_ip = client.containers.get(slave_db.attrs['Name']).attrs['NetworkSettings']['IPAddress']
    slave_app = client.containers.run("app_new:latest",environment=["MYSQL_IP_ADDR="+str(slave_db_ip),"RABBITMQ_IP="+str(rabbitmq_ip)],restart_policy={"Name": "on-failure"},detach=True)
    slave_name = slave_app.attrs['Name']
    app_db_mapping[slave_name]=[slave_app,slave_db]
    time.sleep(20)
    command = "echo "+str(slave_app.top()["Processes"][0][1])+" | cat > /app/mypid.txt"
    os.system("docker exec -i "+slave_name+" /bin/bash -c '"+command+"'")
    os.system("docker cp ./replica.txt "+slave_name.strip("/")+":/app/replica.txt")
        
    print("New worker is deployed")        
    watch_worker()


#------------------------------------------- Watch functions ----------------------------------------------

#Watch function
def inter_respawn(event):
    data, stat = zk.get("/election")
    if(int(data.decode('utf-8')) > len(zk.get_children("/election"))):
        t = threading.Thread(target=respawn_slave)
        t.start()
    

#Set watch on previous znode in increasing order of PIDs
def watch_worker():
    for i in zk.get_children("/election"):
        zk.exists("/election/"+str(i),watch=inter_respawn)


#-------------------------------------------- Autoscaling ------------------------------------------------

#Stop and remove worker with highest PID
def scale_in():
    to_kill = max([app_db_mapping[i][0].top()["Processes"][0][1] for i in app_db_mapping])
    for i in app_db_mapping:
        if(to_kill == app_db_mapping[i][0].top()["Processes"][0][1]):
            app_db_mapping[i][0].stop()
            app_db_mapping[i][1].stop()
            app_db_mapping[i][0].remove()
            app_db_mapping[i][1].remove()
            temp = i
    del(app_db_mapping[temp])


#Scale based on number of read requests
def autoscale():
    val = 0
    minutes = 0
    first_check = 0
    global read_req
    
    #Wait for initial request to start autoscaling
    while(not first_check):
        first_check=read_req

    print("Found request to start")

    #Thread runs for 20 minutes
    while(minutes!=20):

    	#Increment count every second
        if(not val == 120):
            val += 1
            time.sleep(1)

        #After 2 minutes
        else:

        	#Get number of workers present
            found = len(zk.get_children("/election"))
            
            inter_num= read_req
            print(inter_num)
            req_count = (inter_num//21)+1+1
            
            read_req=0

            minutes += 2
            val=0

            #Set new required number of workers
            zk.set("/election",str(req_count).encode())

            #Autoscale
            if(found > req_count):
                print("found {0} need {1} removing {2}".format(found,req_count,found-req_count))
                for i in range(found - req_count):
                    scale_in()
            else:
                print("found {0} need {1} adding {2}".format(found,req_count,-found+req_count))                
                for i in range(req_count - found):
                    t = threading.Thread(target=respawn_slave)
                    t.start()
                    
    print("Stopping autoscaling")
    

#---------------------------------------------- MAIN ----------------------------------------------------    

if __name__=='__main__':
    
    #Get docker client
    client = docker.from_env()
    client2 = docker.APIClient(base_url='unix://var/run/docker.sock')

    #Zookeeper client
    zk = KazooClient(hosts='172.17.0.3:2181')
    zk.start()
    zk.delete("/election",recursive=True)

    #Mappings of slave application containers to corresponding database containers
    app_db_mapping = {}
    
    #Add "election" znode
    if(not zk.ensure_path("/election")):
        zk.create("/election")
        
    #Initial number of workers
    zk.set("/election",b"2")

    open("replica.txt","w").close()
    
    print("Zookeeper ready for election ")    
    rabbitmq_ip = '172.17.0.2'
    unique_id='1'
    container_maps={}

    #Spawn 2 workers
    for _ in range(2):
        slave_db = client.containers.run("mysql_new:latest",environment=["MYSQL_ROOT_PASSWORD=root","MYSQL_DATABASE=cloud_ass3"],detach=True)
        slave_db_ip = client.containers.get(slave_db.attrs['Name']).attrs['NetworkSettings']['IPAddress']
        slave_app = client.containers.run("app_new:latest",environment=["MYSQL_IP_ADDR="+str(slave_db_ip),"RABBITMQ_IP="+str(rabbitmq_ip)],restart_policy={"Name": "on-failure"},detach=True)
        slave_name = slave_app.attrs['Name']
        container_maps[slave_name]=slave_app
        app_db_mapping[slave_name]=[slave_app,slave_db]
        
    print(container_maps)
    
    time.sleep(20)

    #Copy sql commands and PID of worker into the container
    for i in container_maps:
        command = "echo "+str(container_maps[i].top()["Processes"][0][1])+" | cat > /app/mypid.txt"
        os.system("docker exec -i "+i+" /bin/bash -c '"+command+"'")
        os.system("docker cp ./replica.txt "+i.strip("/")+":/app/replica.txt")

    while(not len(zk.get_children("/election")) == 2):
        pass
    
    watch_worker()

    #Thread for autoscaling
    print("Starting thread for autoscaling")
    t1 = threading.Thread(target=autoscale)
    t1.start()
    
    #RabbitMQ connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=str(rabbitmq_ip),heartbeat=0))
    channel = connection.channel()

    #Declare readQ, writeQ, responseQ
    channel.queue_declare(queue='readq', durable=True)
    channel.queue_declare(queue='writeq', durable=True)
    channel.queue_declare(queue='responseq', durable=True)

    #Running Flask app
    app.run(host='0.0.0.0',port=8000)



    





    

