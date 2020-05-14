import mysql.connector
import requests
from flask import Flask, render_template, jsonify, request, abort
import datetime 
import string
import pandas


app=Flask(__name__)    

@app.route('/api/v1/users',methods=["PUT"])
def add_user():

    r= requests.get("http://0.0.0.0:8080/api/v1/incr_count_req")
    username = request.get_json()['username']
    password = request.get_json()['password']

    if(len(password)!=40 or username==''):
        abort(400)
    else:
        for i in password:
            if(i not in string.hexdigits):
                abort(400)


    r = requests.post('http://54.162.181.248:80/api/v1/db/read', json={"column":["username"],"table":"users","where":["username='"+str(username)+"'"]})

    if(r.status_code==400):
        requests.post('http://54.162.181.248:80/api/v1/db/write', json={"insert":[str(username),str(password)],"column":["username","password"],"table":"users"})
        return 'Successfully created user', 201
    
    else:
        abort(400)


@app.route("/api/v1/users/<username>",methods=["DELETE"])
def remove_user(username):
    r= requests.get("http://0.0.0.0:8080/api/v1/incr_count_req")    
    r = requests.post('http://54.162.181.248:80/api/v1/db/read', json={"column":["username"],"table":"users","where":["username='"+str(username)+"'"]})
    
    if(r.status_code==400):
        return 'error',400
    else:
        sql_query = "DELETE from users WHERE username='"+str(username)+"';"
        r = requests.post('http://54.162.181.248:80/api/v1/db/delop', json={"query":sql_query})
        return '',200

@app.route("/api/v1/users")
def list_users():

    r= requests.get("http://0.0.0.0:8080/api/v1/incr_count_req")
    
    res = []
    r = requests.post('http://54.162.181.248:80/api/v1/db/read', json={"column":["username"],"table":"users","where":["username is NOT NULL"]})
    for i in eval(r.text):
        res.append(i[0])
    if(len(res)):
        return str(res),200
    else:
        return '',204


@app.route("/api/v1/incr_count_req")
def incr_count_req():
    f = open("count_reqs.txt",'r+')
    rep = int(f.readline()) + 1
    f.close()
    f = open("count_reqs.txt",'w+')
    f.write(str(rep))
    f.close()
    return '',200

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

@app.errorhandler(405)
def not_allowed(temp):
    if('/api/v1/db/clear' in temp.name or 'count' in temp.name):
        return str(temp),405
    r= requests.get("http://0.0.0.0:8080/api/v1/incr_count_req")    
    return str(temp),405

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8080)


