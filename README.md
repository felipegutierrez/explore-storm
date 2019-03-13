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

Start UI `apache-storm-1.2.2/bin/storm ui`

Connect to http://127.0.0.1:8080 . During initializing all the services it is a good idea monitor the log files for each one of them.
```
tail -f apache-storm-1.2.2/logs/nimbus.log
tail -f apache-storm-1.2.2/logs/supervisor.log
tail -f apache-storm-1.2.2/logs/ui.log
```

## Running a Topology

Export the project as a Jar file using Eclipse IDE: `Right button on the project` > `Export` > `Runnable JAR file` > `Copy required libraries into sub-folder next to the generated JAR` > `Finish`.

Copy libraries to the Storm lib path
```
cd eclipse-workspace/target/explore-storm_lib/
cp -Rf hawtdispatch-transport-1.22.jar hawtdispatch-1.22.jar hawtbuf-1.11.jar sigar-1.6.4.jar ../../Server/storm/datadir/storm/apache-storm-1.2.2/lib/
```
Deploy the topology on the cluster. The program will ask which application you want to run and if you want to deploy on the cluster (type CLUSTER) or run locally (type LOCAL).
```
apache-storm-1.2.2/bin/storm jar /home/felipe/eclipse-workspace/explore-storm/target/explore-storm.jar org.sense.storm.App
```
Visualize the output on one of the workers log.
```
tail -f apache-storm-1.2.2/logs/workers-artifacts/MqttSensorAnalyserStorm-3-1551877355/6701/worker.log
```

If an error occuors in the topology you may want to fix the error and re-deploy it on the cluster. However, you have to remove the topology which has an error. For this you have to execute the command bellow:
```
storm kill MqttSensorAnalyserStorm [-w wait-time-secs]
```

## Using the ResourceAwareScheduler (RAS) from Storm

This is the reference of the [Resource Aware Scheduler](http://storm.apache.org/releases/1.2.2/Resource_Aware_Scheduler_overview.html) in Storm.

Update the `storm.yaml` file with the RAS class. Set the amount of memory for the workers. 

```
storm.scheduler: "org.apache.storm.scheduler.resource.ResourceAwareScheduler"
# default value for the max heap size for a worker
topology.worker.max.heap.size.mb: 2048.0
```

Make sure that the sum of all memory set on the method `setMemoryLoad()` does not exceed the amount of memory set on `topology.worker.max.heap.size.mb`.

## Adding an extendable scheduler on Storm

To add an extendable scheduler on Storm you have to implement the `IScheduler` interface. This project has an example on the file [TagAwareScheduler.java](https://github.com/felipegutierrez/explore-storm/blob/master/src/main/java/org/sense/storm/scheduler/TagAwareScheduler.java). Then, you have to add the following lines on the file `apache-storm-1.2.2/conf/storm.yaml`:
```
supervisor.scheduler.meta:
    tags: GPU
storm.scheduler: "org.sense.storm.scheduler.TagAwareScheduler"
```
And restart all the storm services on the cluster





## References

- [Taking control of your Apache Storm cluster with tag-aware scheduling](https://inside.edited.com/taking-control-of-your-apache-storm-cluster-with-tag-aware-scheduling-b60aaaa5e37e)
- [Metadata-Aware Scheduler for Apache Storm](https://dcvan24.wordpress.com/2015/04/07/metadata-aware-custom-scheduler-in-storm/)
- [Setting up a single node Apache Storm cluster](https://medium.com/real-time-streaming/setting-up-a-single-node-apache-storm-cluster-3dda02add2e9)
- [5 minutes Storm installation guide (single-node setup)](https://vincenzogulisano.com/2015/07/30/5-minutes-storm-installation-guide-single-node-setup/)
- [Understanding the Parallelism of a Storm Topology](https://www.michael-noll.com/blog/2012/10/16/understanding-the-parallelism-of-a-storm-topology/)
- [Resource Aware Scheduler](http://storm.apache.org/releases/1.2.2/Resource_Aware_Scheduler_overview.html)




