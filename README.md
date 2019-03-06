This project is based on [Apache Storm](http://storm.apache.org/) and it is consuming data from another project [https://github.com/felipegutierrez/explore-rpi](https://github.com/felipegutierrez/explore-rpi) which is based on [Apache Edgent](http://edgent.apache.org/).


## Configuring Apache Storm on your local machine (single-node setup)

If you wish to run Storm on a cluster you have to set Zookeeper and Nimbus. Afterwards deploy the jar file on the Storm cluster.

Create a directory to download storm and zookeeper
```
mkdir storm
cd storm
mkdir -p datadir/zookeeper
wget http://ftp.halifax.rwth-aachen.de/apache/zookeeper/stable/zookeeper-3.4.13.tar.gz
tar -xvf zookeeper-3.4.13.tar.gz
wget http://www.apache.org/dyn/closer.lua/storm/apache-storm-1.2.2/apache-storm-1.2.2.tar.gz
tar -xvf apache-storm-1.2.2.tar.gz
```

Configure ZooKeeper. Add the following lines to zookeeper-3.4.13/conf/zoo.cfg.
```
tickTime=2000
dataDir=/home/username/datadir/zookeeper
clientPort=2181
```

Configure Storm. Uncomment/add the following to apache-storm-1.2.2/conf/storm.yaml
```
storm.zookeeper.servers:
    - "127.0.0.1"
nimbus.host: "127.0.0.1"
storm.local.dir: "/home/username/storm/datadir/storm"
supervisor.slots.ports:
    - 6700
    - 6701
    - 6702
    - 6703
```

Start ZooKeeper `zookeeper-3.4.13/bin/zkServer.sh start`

Start nimbus `apache-storm-1.2.2/bin/storm nimbus`

Start supervisor `apache-storm-1.2.2/bin/storm supervisor`

Start UI `apache-storm-0.9.5/bin/storm ui`

Connect to http://127.0.0.1:8080 .

## Running a Topology

Export the project as a Jar file using Eclipse IDE.
```
Right button on the project > Export > Runnable JAR file > Copy required libraries into sub-folder next to the generated JAR > Finish.
```
Copy libraries to the Storm lib path
```
cp -Rf explore-storm_lib/hawtdispatch-transport-1.22.jar explore-storm_lib/hawtdispatch-1.22.jar explore-storm_lib/hawtbuf-1.11.jar apache-storm-1.2.2/lib/
```
Deploy the topology on the cluster. The program will ask which application you want to run and if you want to deploy on the cluster (type CLUSTER) or run locally (type LOCAL).
```
apache-storm-1.2.2/bin/storm jar /home/felipe/eclipse-workspace/explore-storm/target/explore-storm.jar org.sense.storm.App
```
Visualize the output on one of the workers log.
```
tail -f apache-storm-1.2.2/logs/workers-artifacts/MqttSensorAnalyserStorm-3-1551877355/6701/worker.log
```





