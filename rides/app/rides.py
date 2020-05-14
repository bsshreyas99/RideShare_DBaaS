import mysql.connector
import requests
from flask import Flask, render_template, jsonify, request, abort
import datetime 
import string
import pandas


app=Flask(__name__)    


@app.route("/api/v1/rides",methods=['POST'])
def add_ride():

    r = requests.get('http://0.0.0.0:8000/api/v1/incr_count_reqs')
    
    created_by = request.get_json()['created_by']
    ts = request.get_json()['timestamp']
    src = request.get_json()['source']
    dest = request.get_json()['destination']

    
    if(int(src) not in range(1,199) or int(dest) not in range(1,199)):
        abort(400)
    
    dateTimeObj= datetime.datetime.strptime(ts, '%d-%m-%Y:%S-%M-%H')
    time_stamp = dateTimeObj.strftime("%Y-%m-%d %H:%M:%S")
    
    #r = requests.get('http://18.213.73.18:8080/api/v1/users')
    headers={'Origin':'http://3.214.183.110:80'}
    r = requests.get('http://RideShare-229727897.us-east-1.elb.amazonaws.com/api/v1/users', headers=headers)
    
    if(r.text):
        all_user = eval(r.text)
    else:
        abort(400)

    if(r.status_code==400):
        abort(400)

    else:
        if(str(created_by) in all_user):
            r = requests.post('http://54.162.181.248:80/api/v1/db/write', json={"insert":[str(created_by),str(created_by),str(src),str(dest),str(time_stamp)],"column":["created_by","users","src","dest","ts"],"table":"rides"})

            
    return 'Successfully created new ride',201



@app.route("/api/v1/rides")
def list_rides():
    
    r = requests.get('http://0.0.0.0:8000/api/v1/incr_count_reqs')
    
    src = request.args.get('source', default = 1, type = int)
    dest = request.args.get('destination', default = 1, type = int)

    if(int(src) not in range(1,199) or int(dest) not in range(1,199)):
        abort(400)

    cur_ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    r = requests.post('http://54.162.181.248:80/api/v1/db/read', json={"column":["rideid","created_by","users","ts"],"table":"rides","where":["src='"+str(src)+"'","dest='"+str(dest)+"'","ts>'"+str(cur_ts)+"'"]})

    if(r.status_code==400):
        return '',204
    
    else:
        x= eval(r.text)
        res=[]
        for i in x:
            if(i[1]==i[2]):
                d={}
                d["rideId"]=i[0]
                d["username"]=i[1]
                d["timestamp"]=i[3].strftime("%d-%m-%Y:%S-%M-%H")

                res.append(d)

        
        return jsonify(res),200


@app.route("/api/v1/rides/<rideId>")
def list_details(rideId):
    r = requests.get('http://0.0.0.0:8000/api/v1/incr_count_reqs')
    r = requests.post('http://54.162.181.248:80/api/v1/db/read', json={"column":["rideid","created_by","users","ts","src","dest"],"table":"rides","where":["rideid='"+str(rideId)+"'"]})
    flag=0
    if(r.status_code==400):
        abort(400)
    else:
        x= eval(r.text)
        d={}
        
        d["rideId"]=x[0][0]
        d["created_by"]=x[0][1]
        d["users"]=[]
        d["timestamp"]=x[0][3].strftime("%d-%m-%Y:%S-%M-%H")
        d["source"]=x[0][4]
        d["destination"]=x[0][5]
        data= pandas.read_csv('AreaNameEnum.csv')

        for i in x:
            if(i[2]!=d["created_by"]):
                d["users"].append(i[2])
                

        
        return jsonify(d),200


@app.route("/api/v1/rides/<rideId>", methods=["POST"])
def join_ride(rideId):
    r = requests.get('http://0.0.0.0:8000/api/v1/incr_count_reqs')
    username = request.get_json()['username']

    r = requests.post('http://54.162.181.248:80/api/v1/db/read', json={"column":["rideid","created_by","ts","src","dest"],"table":"rides","where":["rideid='"+str(rideId)+"'"]})
    r2= requests.post('http://54.162.181.248:80/api/v1/db/read', json={"column":["rideid","created_by"],"table":"rides","where":["rideid='"+str(rideId)+"'", "users='"+str(username)+"'"]})
    
    #r1 = requests.get('http://18.213.73.18:8080/api/v1/users')
    headers={'Origin':'http://3.214.182.110:80'}
    r1 = requests.get('http://RideShare-229727897.us-east-1.elb.amazonaws.com/api/v1/users', headers=headers)
    
    if(r1.text):
        x=eval(r1.text)

    if(r.status_code==400 or (username not in x)  or r2.status_code!=400):
        abort(400)
    else:
        x= eval(r.text)
        dateTimeObj = x[0][2]
     
        time_stamp = dateTimeObj.strftime("%Y-%m-%d %H:%M:%S")
        created_by = x[0][1]
        src=x[0][3]
        dest=x[0][4]
        r = requests.post('http://54.162.181.248:80/api/v1/db/write', json={"insert":[str(rideId),str(created_by),str(username),str(src),str(dest),str(time_stamp)],"column":["rideid","created_by","users","src","dest","ts"],"table":"rides"})
        return '',200


@app.route("/api/v1/rides/<rideId>", methods=["DELETE"])
def delete_ride(rideId):
    
    r = requests.get('http://0.0.0.0:8000/api/v1/incr_count_reqs')
    
    r = requests.post('http://54.162.181.248:80/api/v1/db/read', json={"column":["rideid"],"table":"rides","where":["rideid='"+str(rideId)+"'"]})
    
    if(r.status_code==400):
        abort(400)
    else:
        sql_query = "DELETE from rides WHERE rideid='"+str(rideId)+"';"
        r = requests.post('http://54.162.181.248:80/api/v1/db/delop', json={"query":sql_query})
        return '',200



@app.route("/api/v1/rides/count")
def ride_count():
    r = requests.get('http://0.0.0.0:8000/api/v1/incr_count_reqs')
    res=[]
    sql_query= "SELECT count(DISTINCT rideid) FROM rides;"
    
    r = requests.post('http://54.162.181.248:80/api/v1/db/read', json={"column":["count(DISTINCT rideid)"],"table":"rides","where":["rideid is NOT NULL"]})
    
    #column count(DISTINCT rideid)
    #table rides
    #where rideid is NOT NULL
    
    return str(eval(r.text)[0][0]),200

@app.route("/api/v1/_count")
def http_count():
    f = open("count_reqs.txt","r+")
    ret = int(f.readline())
    f.close()
    return str([ret]),200

@app.route("/api/v1/_count",methods=["DELETE"])
def http_count2():
    f = open("count_reqs.txt",'w+')
    f.write(str(0))
    f.close()
    return '',200

@app.route("/api/v1/incr_count_reqs")
def incr_count_reqs():
    f = open("count_reqs.txt",'r+')
    rep = int(f.readline()) + 1
    f.close()
    f = open("count_reqs.txt",'w+')
    f.write(str(rep))
    f.close()
    return '',200

@app.errorhandler(405)
def not_allowed(temp):
    if('/api/v1/db/clear' in temp.name or 'count' in temp.name):
        return str(temp),405
    r = requests.get('http://0.0.0.0:8000/api/v1/incr_count_reqs')
    return str(temp),405

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0',port=8000)


