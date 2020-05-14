# RideShare_DBaaS

Create three instances on AWS or any other cloud service with Ubuntu as the operating system. Install docker on all the instances.
* Copy the **orchestrator**, **worker**, **database**, **rabbitmq**, **zookeeper** folders to the first instance.
* Copy the **rides** folder to the second instance.
* Copy the **users** folder to the third instance.

### Instance 1:
1. Inside the orchestrator folder, run the command `sudo docker built -t orc_new ./` to create the orchestrator image.
2. Inside the worker folder, run the command `sudo docker built -t app_new ./` to create the worker application image.
3. Inside the database folder, run the command `sudo docker built -t mysql_new ./` to create the mysql database image.
4. Inside the rabbitmq folder, run the command `sudo docker built -t rabbitmq_new ./` to create the rabbitmq image.
5. Inside the zookeeper folder, run the command `sudo docker built -t zoo_new ./` to create the zookeeper image.
6. Run the rabbitmq container with the created image using the command `sudo docker run -p 5672:5672 rabbitmq_new`.
7. Run the zookeeper container with the created image using the command `sudo docker run -p 2181:2181 zoo_new`.
8. Finally run the orchestrator container by mounting the volume `/var/run/docker.sock`. Use the following command to achieve this:
  `sudo docker run -it -p 80:8000 -v /var/run/docker.sock:/var/run/docker.sock -v /usr/bin:/usr/bin orc_new`. The read and      write APIs are exposed on port 80.

### Instance 2 and 3:
1. Bring up the **users** and **rides** containers by running their respective *.yml* files using the command `sudo docker-compose up --build`.


After a wait of about 30 seconds, the master and slave containers are spawned and the application is ready to serve client requests.





