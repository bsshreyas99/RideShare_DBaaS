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
read_req=0

@app.route("/api/v1/db/clear",methods=["POST"])
def clear_db():
    sql_query = "DELETE FROM users"
    
    replica_file = open("replica.txt","a")
    replica_file.write(sql_query+"\n")
    replica_file.close()
    
    channel.basic_publish(exchange='', routing_key='writeq', body=sql_query)
    return 'Cleared database',200

@app.route('/api/v1/db/delop',methods=["POST"])
def del_db():
    
    sql_query = request.get_json()['query']
    
    replica_file = open("replica.txt","a")
    replica_file.write(sql_query+"\n")
    replica_file.close()
    
    channel.basic_publish(exchange='', routing_key='writeq', body=sql_query)
    return '',201


@app.route('/api/v1/db/write',methods=["POST"])
def write_db():
    to_ins = request.get_json()['insert'] # [ 'shashank', '6ae999552a0d2dca14d62e2bc8b764d377b1dd6c' ]
    to_col = request.get_json()['column'] #  [ 'username' , 'password' ]
    to_table = request.get_json()['table'] # users

    sql_query = "INSERT INTO "+to_table+" "+str(to_col).replace('[','(').replace(']',')').replace("'","")+" values "+str(to_ins).replace('[','(').replace(']',');')

    replica_file = open("replica.txt","a")
    replica_file.write(sql_query+"\n")
    replica_file.close()
    
    channel.basic_publish(exchange='', routing_key='writeq', body=sql_query)
    return '',201

@app.route('/api/v1/db/read',methods=["POST"])
def read_db():
    global read_req
    to_col = request.get_json()['column']
    to_table = request.get_json()['table']
    to_conds = request.get_json()['where']

    sql_query = "SELECT " + str(to_col).replace('[',"").replace("]","").replace("'","") + " "
    sql_query += "FROM " + str(to_table) + " "
    if(len(to_conds)==1):
        sql_query += "WHERE "+to_conds[0]
    else:
        sql_query += "WHERE " + str(to_conds).replace('[',"").replace("]","").replace("\"","").replace(","," and ") + ";"
    print(sql_query)
    
    read_req+=1
    
    channel.basic_publish(exchange='',
                      routing_key='readq',
                      body=sql_query,
                      properties=pika.BasicProperties(delivery_mode=2))

    def callback(ch,method,properties,body):
        global return_str
        return_str = body.decode('utf-8')
        channel.stop_consuming()

    
    channel.basic_consume(queue="responseq", on_message_callback=callback)
    channel.start_consuming()
    print(return_str)
    if(eval(return_str)):
        return str(return_str),200
    abort(400)


@app.route('/api/v1/crash/slave',methods=["POST"])
def crash_slave():
    to_ret = int(max([app_db_mapping[i][0].top()["Processes"][0][1] for i in app_db_mapping]))
    scale_in()
    return str([to_ret]),200

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

@app.route('/api/v1/worker/list',methods=["GET"])
def list_worker():
    workers = []
    for i in app_db_mapping:
        workers.append(int(app_db_mapping[i][0].top()["Processes"][0][1]))
    return str(sorted(workers)),200

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

def inter_respawn(event):
    data, stat = zk.get("/election")
    if(int(data.decode('utf-8')) > len(zk.get_children("/election"))):
        t = threading.Thread(target=respawn_slave)
        t.start()
    
def watch_worker():
    for i in zk.get_children("/election"):
        zk.exists("/election/"+str(i),watch=inter_respawn)

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

def autoscale():
    val = 0
    minutes = 0
    first_check = 0
    global read_req
    
    while(not first_check):
        first_check=read_req

    print("found request to start")
    while(minutes!=20):
        if(not val == 120):
            val += 1
            time.sleep(1)
        else:
            found = len(zk.get_children("/election"))
            
            inter_num= read_req
            print(inter_num)
            req_count = (inter_num//21)+1+1
            
            read_req=0

            minutes += 2
            val=0
            zk.set("/election",str(req_count).encode())
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
    
if __name__=='__main__':
    
    
    client = docker.from_env()
    client2 = docker.APIClient(base_url='unix://var/run/docker.sock')
    zk = KazooClient(hosts='172.17.0.3:2181')
    zk.start()
    zk.delete("/election",recursive=True)

    app_db_mapping = {}
    
    if(not zk.ensure_path("/election")):
        zk.create("/election")
        
    zk.set("/election",b"2")

    open("replica.txt","w").close()
    
    print("Zookeeper ready for election ")    
    rabbitmq_ip = '172.17.0.2'
    unique_id='1'
    container_maps={}
    for _ in range(2):
        slave_db = client.containers.run("mysql_new:latest",environment=["MYSQL_ROOT_PASSWORD=root","MYSQL_DATABASE=cloud_ass3"],detach=True)
        slave_db_ip = client.containers.get(slave_db.attrs['Name']).attrs['NetworkSettings']['IPAddress']
        slave_app = client.containers.run("app_new:latest",environment=["MYSQL_IP_ADDR="+str(slave_db_ip),"RABBITMQ_IP="+str(rabbitmq_ip)],restart_policy={"Name": "on-failure"},detach=True)
        slave_name = slave_app.attrs['Name']
        container_maps[slave_name]=slave_app
        app_db_mapping[slave_name]=[slave_app,slave_db]
        
    print(container_maps)
    
    time.sleep(20)
    for i in container_maps:
        command = "echo "+str(container_maps[i].top()["Processes"][0][1])+" | cat > /app/mypid.txt"
        os.system("docker exec -i "+i+" /bin/bash -c '"+command+"'")
        os.system("docker cp ./replica.txt "+i.strip("/")+":/app/replica.txt")

    while(not len(zk.get_children("/election")) == 2):
        pass
    
    watch_worker()

    print("Starting thread for autoscaling")
    t1 = threading.Thread(target=autoscale)
    t1.start()
    
    
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=str(rabbitmq_ip),heartbeat=0))
    channel = connection.channel()

    channel.queue_declare(queue='readq', durable=True)
    channel.queue_declare(queue='writeq', durable=True)
    channel.queue_declare(queue='responseq', durable=True)
    app.run(host='0.0.0.0',port=8000)



    





    

