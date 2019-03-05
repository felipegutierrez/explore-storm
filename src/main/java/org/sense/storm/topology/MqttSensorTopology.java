package org.sense.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.sense.storm.spout.MqttSensorSpout;

public class MqttSensorTopology {

	public MqttSensorTopology() throws Exception {
		// Create Config instance for cluster configuration
		Config config = new Config();
		config.setDebug(false);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("call-log-reader-spout", new MqttSensorSpout("topic-station-01"));

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
		Thread.sleep(100000);

		// Stop the topology

		cluster.shutdown();
	}
}
