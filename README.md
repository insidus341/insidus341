# James Earl - Coding Assignment

This project was created to analyze a stream of data from a Kafka topic and insert calculated stats into a database.

## Getting Started

These instructions will get you a copy of the project up and running on a virtual machine for testing purposes. See deployment for notes on how to deploy this project.

### Prerequisites

* Linux Debian 10
* Java JRE 11.0.6
* Java JDK 11.0.6
* Python 3.7.3
* Apache Kafka 2.4.1
* MariaDB 10.3.22


PIP3
* kafka==1.3.5
* mysql-connector==2.2.9
* mysql-connector-python==8.0.19

### Installing

A step by step series of examples that tell you how to get a development environment running

Optionally, you can run Debian10 in a Virtual Machine with VMWare Player 15

Once Debian is installed, let's create give our user sudo access. I will assume the user is 'james'
```
su root
apt-get update && apt-get install sudo
sudo -s
usermod -aG sudo james
```

To install Java JRE and JDK
https://www.digitalocean.com/community/tutorials/how-to-install-java-with-apt-on-debian-10#installing-the-default-jrejdk

```
sudo apt update
sudo install default-jre default-jdk
```

To install Python3
```
sudo apt-get install python3
```

To install Mariadb
https://www.digitalocean.com/community/tutorials/how-to-install-mariadb-on-debian-10
```
sudo apt-get install mariadb-server
sudo mysql_secure_installation
(set a new password for root, delete the testing databases and flush privileges) 
```

To install Apache Kafka
https://www.digitalocean.com/community/tutorials/how-to-install-apache-kafka-on-debian-10
```
wget https://archive.apache.org/dist/kafka/2.4.1/kafka_2.11-2.4.1.tgz
tar -xvzf ~/kafka_2.11-2.4.1.tgz --strip 1

```

Create a service file for Zookeeper
```
sudo nano /etc/systemd/system/zookeeper.service

[Unit]
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
User=james
ExecStart=/home/james/kafka/bin/zookeeper-server-start.sh /home/james/kafka/config/zookeeper.properties
ExecStop=/home/james/kafka/bin/zookeeper-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
```

Create a service file for Kafka
```
sudo nano /etc/systemd/system/kafka.service

[Unit]
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
User=james
ExecStart=/bin/sh -c '/home/james/kafka/bin/kafka-server-start.sh /home/james/kafka/config/server.properties > /home/kafka/kafka/kafka.log 2>&1'
ExecStop=/home/james/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
```

Enable and start Kafka
```
sudo systemctl enable kafka
sudo systemctl start kafka

sudo journalctl -u kafka
output:
Mar 28 13:31:48 kafka systemd[1]: Started kafka.service.
```

Create the Kafka Topic
```
/home/james/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic CodingAssignment
```

#### Pull the project from Github
```
https://codeload.github.com/insidus341/James-Earl---Coding-Assignment/zip/master

 - if unzip is not installed: sudo apt-get install unzip
unzip master
cd James-Earl---Coding-Assignment-master
```

Install the PIP3 requirements
```
pip3 install -r requirements/requirements.txt
```

Create the Database, Table and User
```
sudo mysql -u root -p < database.txt
```

#### Setup Confirmation
Confirm Kafka
```
systemctl status kafka | grep active
/home/james/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181 | grep CodingAssignment
```

Confirm the Database
```
sudo mysql -u root -p
USE CodingAssignment;
SHOW COLUMNS from NodeCheckIn;
```

## Run

The easiest way to run this is with two console Windows open.
You can find the IP address of Debian with `ip addr`. SSH into Debian from an SSH client (Putty) twice.

To begin with, the stream will be empty. In one Window, run `python3 run/app.py`. The Window will remain blank as there's no data.

One the second console window, run `python3 run/simulate_input.py`. This will throw a bunch of messages onto the screan.
These will appear on the first console window running app.py. As long as the messages from simulate_input
are shown on the console for app.py we are reading the stream data.

Once this has been running for a few minutes the database will be populated. We can view the database
by running: 
```
sudo mysql -u root -p
USE CodingAssignment;
SELECT * FROM NodeCheckIn;
```

```
MariaDB [CodingAssignment]> SELECT * FROM NodeCheckIn;
+-----+----------------+-------+-----------+-----------+-----------+---------------------+
| ID  | Node_ID        | Count | Min_Value | Max_Value | Avg_Value | Occured             |
+-----+----------------+-------+-----------+-----------+-----------+---------------------+
|  21 | 12345678900014 |  4853 |        15 |    999980 |    501743 | 2020-03-28 18:44:42 |
|  22 | 12345678900018 |  4741 |       114 |    999883 |    499137 | 2020-03-28 18:44:42 |
|  23 | 12345678900001 |  4822 |       155 |    999848 |    496389 | 2020-03-28 18:44:42 |
|  24 | 12345678900005 |  4716 |       364 |    999867 |    498524 | 2020-03-28 18:44:42 |
|  25 | 12345678900002 |  4851 |       484 |    999948 |    500410 | 2020-03-28 18:44:42 |
|  26 | 12345678900007 |  4797 |       158 |    999875 |    497256 | 2020-03-28 18:44:42 |
+-----+----------------+-------+-----------+-----------+-----------+---------------------+

```

## Built With

* [Apache Kafka](https://kafka.apache.org/) - Distributed Streaming Platform
* [MariaDC](https://mariadb.org/) - Open Source Relational Database
* [MySQL Connector](https://dev.mysql.com/downloads/connector/python/) - Used by Python3 for Database Access

## Authors

* **James Earl** - *Project* - [Insidus341](https://github.com/insidus341)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Digital Ocean for the setup guides