# Web-Crawling
This repository is created for crawling several Iranian's news agency websites using Apache Storm. In src directory each website has one spout file and one bolt file. In Storm structure spout is responsible for fetch data from resources and bolt is responsible to fetch and process data. Here we want to read links from csv files then pass them to bolt to parse html pages and extract data then store in Apache Solr. Follow bellow steps to run it:

##Install Apache-Storm-2.2.0 for running clusters.
###Step 1 — Verify Java:
Use the following command to check whether you have Java already installed on your system.

	$ java -version

	output:
	openjdk version  "11.0.10"
	OpenJDK Runtime Environment (build 1.8.0_222-8u222-b10-1ubuntu1~18.04.1-b10)
	OpenJDK 64-Bit Server VM (build 25.222-b10, mixed mode)


If Java is already there, then you would see its version number. Else, download and install the latest version of JDK.

### Step 2 — Install Apache ZooKeeper:
To install the ZooKeeper framework on your machine, visit the following link and download ZooKeeper . In this project we are using ZooKeeper 3.4.14 (ZooKeeper-3.4.14.tar.gz). http://zookeeper.apache.org/releases.html
Extract the tar file using the following commands:

	$ cd /opt

	$ sudo chmod 777 /opt/ -R 

	$ wget http://mirror.23media.de/apache/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz

	$ tar -zxf zookeeper-3.4.14.tar.gz# cd zookeeper-3.4.14

	$ mkdir data


Open configuration file named conf/zoo.cfg using the command nano conf/zoo.cfg and setting all the following parameters as starting point.

	$ nano conf/zoo.cfg

	tickTime=2000
	dataDir=/opt/zookeeper-3.4.14/data
	clientPort=2181
	initLimit=5
	syncLimit=2


Once the configuration file has been saved successfully, you can start the ZooKeeper server. Use the following command to start the ZooKeeper server. After executing this command, you will get a response as follows:

	$ bin/zkServer.sh start
	
	output:
	ZooKeeper JMX enabled by default
	Using config: /opt/zookeeper-3.4.14/bin/../conf/zoo.cfg
	Starting zookeeper ... STARTED




### Step 3 — Install Apache Storm:
To install the Storm framework on your machine, visit the following link and download Storm . In this project we are using Storm 2.2.0 (Storm-2.2.0.tar.gz). 
http://storm.apache.org/downloads.html
Step3.1 Extract the tar file using the following commands and make data directory:
	$ wget https://downloads.apache.org/storm/apache-storm-2.2.0/apache-storm-2.2.0.tar.gz

	$ tar -zxf apache-storm-2.2.0.tar.gz
	$ cd apache-storm-2.2.0
	$ mkdir data

#### Step 3.2 The current release of Storm contains a file at conf/storm.yaml that configures Storm daemons. 
	$ nano conf/storm.yaml

	Add the following information to that file.
	storm.zookeeper.servers:
	- "localhost"
	storm.local.dir: "/opt/apache-storm-2.2.0/data"
	nimbus.host: "localhost"
	supervisor.slots.ports:
	- 6700
	- 6701
	- 6702

	ui.port : 8090
	ui.host: "192.168.99.156"
	topology.subprocess.timeout.secs: 100000
	topology.message.timeout.secs: 600
	topology.executor.receive.buffer.size: 16384
	topology.executor.send.buffer.size: 16384
	topology.acker.executors: 3
	topology.max.spout.pending: 3
	topology.sleep.spout.wait.strategy.time.ms: 3000
	topology.component.resources.onheap.memory.mb: 128.0
	topology.component.resources.offheap.memory.mb: 0.0
	topology.component.cpu.pcore.percent: 10.0
	worker.heap.memory.mb: 2048
	worker.childopts: "-Xmx%HEAP-MEM%m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=artifacts/heapdump"
	worker.gc.childopts: ""

 #### *Note: change ui.host: "192.168.99.156" to your ip address.
After applying all the changes, save and return to the terminal.
####Step  3.3 Adding storm/bin to PATH:
Open the bashrc file and add the following line to the end of it, then save and exit.
	$ nano ~/.bashrc

	 export PATH=/opt/apache-storm-2.2.0/bin${PATH:+:${PATH}}

	$ source ~/.bashrc

Now you can run the storm commands in terminal, for check it run this command and you can see output like these lines:
	$ storm version

	output:
	Running: java -client -Ddaemon.name= -Dstorm.options= -Dstorm.home=/opt/apache-storm-2.2.0 -Dstorm.log.dir=/opt/apache-storm-2.2.0/logs -Djava.library.path=/usr/local/lib:/opt/local/lib:/usr/lib:/usr/lib64 -Dstorm.conf.file= -cp /opt/apache-storm-2.2.0/*:/opt/apache-storm-2.2.0/lib/*:/opt/apache-storm-2.2.0/extlib/*:/opt/apache-storm-2.2.0/extlib-daemon/*:/opt/apache-storm-2.2.0/conf org.apache.storm.utils.VersionInfo
	Storm 2.2.0
	URL https://gitbox.apache.org/repos/asf/storm.git -r bf1986345de5de605abb4fc7b6051fce762bbca5
	Branch (no branch)
	Compiled by ec2-user on 2020-06-18T16:51Z
	From source with checksum c247d5df86c3d159e386b1d5d7827daa
#### *Note: If you have response like this:
bin/storm: line 42: 10 * ‘python’: + ‘python’:: syntax error: operand expected (error token is "‘python’: + ‘python’:")
bin/storm: line 43: ((: < 26 : syntax error: operand expected (error token is "< 26 ")
/usr/bin/env: ‘python’: No such file or directory
It is because of ubuntu 20, run this command to fix it:
$ sudo apt-get install python-is-python3

 #### Step 3.4 − Start the Nimbus
Restart the zookeeper:

	$ bin/zkServer.sh restart
	We must start the Nimbus, Supervisor and UI in separate terminals so create the tmux session before start them.
	$ cd /opt/apache-storm-2.2.0
	$ tmux new -s nimbus
	$ bin/storm nimbus
Note: the short key for exiting from tmux session: ctrl+B+D
	the command for enter the session: $ tmux a -t “name of session”
#### Step 3.5 Start the Supervisor
	$ tmux new -s supervisor
	$ bin/storm supervisor
	Step 3.6 Start the UI
	$ tmux new -s ui
	$ bin/storm ui
After starting the Storm user interface application, type the URL http://localhost:8090 in your favorite browser and you could see Storm cluster information and its running topology. The page should look similar to the following screenshot.


### Step 4 — Install Streamparse:
Streamparse lets you run Python code against real-time streams of data via Apache Storm. With streamparse you can create Storm bolts and spouts in Python. To install this package follow these commands:

#### Step 4.1 Confirm that you have lein installed by running:
	$ sudo apt install leiningen

	$ lein version

	Output:
	Leiningen 2.9.1 on Java 11.0.10 OpenJDK 64-Bit Server VM

#### Step 4.2 Install streamparse

	$ sudo pip install streamparse

### Step 5 — Configuration Streamparse Project:
If you have streamparse project  code you must change “config.json” and “project.clj” files by following below commands, otherwise you can make a new streamparse project.
streamparse projects expect to have the following directory layout:
You need to change “config.json” and “project.clj” files. First you need to make a new directory to save the stream’’s logs.

	$ cd /opt/apache-storm-2.2.0
	$ mkdir streamlogs

Edit the config.json file, you must change the log path:

	 "Log":
		 { "path": "/opt/apache-storm-2.2.0/streamlogs/",

Edit the project.clj file. Replace all lines with below block:

	(require 'cemerick.pomegranate.aether)
	(cemerick.pomegranate.aether/register-wagon-factory!
	 "http" #(org.apache.maven.wagon.providers.http.HttpWagon.))
	(defproject fkjewerlly "0.0.1-SNAPSHOT"
	  :resource-paths ["_resources"]
	  :target-path "_build"
	  :min-lein-version "2.0.0"
	  :jvm-opts ["-client"]
	  :dependencies  [[org.apache.storm/storm-core "2.2.0"]
			[org.apache.storm/flux-core "2.2.0"]]
	  :jar-exclusions 	[#"log4j\.properties" #"org\.apache\.storm\.(?!flux)" #"trident" #"META-INF" #"meta-inf" #"\.yaml"]
	  :uberjar-exclusions [#"log4j\.properties" #"org\.apache\.storm\.(?!flux)" #"trident" #"META-INF" #"meta-inf" #"\.yaml"]
	  )

#### Now you can run your project by command:

	$ sparse run --name ‘name of your topology file’

You can see the summary of all running topologies and etc. at http://localhost:8090. 

### Step 6 — Check the log files:
For checking the logs of worker:

	$ cd /opt/apache-storm-2.2.0
	$ tail -f logs/workers-artifacts/aassttiinn_topology-4-1619430874/6700/worker.log

There are files for every running the topology file and named the same as your topology.

For checking the logs of supervisor:
	$ tail -f logs/supervisor.log

For checking the logs of nimbus:

	$ tail -f logs/nimbus.log


# References
https://www.tutorialspoint.com/apache_storm/apache_storm_installation.htm

https://streamparse.readthedocs.io/en/stable/quickstart.html#

https://medium.com/@AliAzG/scalable-web-crawling-using-stormcrawler-and-apache-solr-cbac70926ccc

**If you see this error:
restart you zookeeper
	$ cd /opt /zookeeper
	$ bin/zkServer.sh restart
