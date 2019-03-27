package org.sense.storm.topology;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.sense.storm.bolt.SensorAggregateValuesWindowBolt;
import org.sense.storm.bolt.SensorPrinterBolt;
import org.sense.storm.spout.MqttSensorDetailSpout;
import org.sense.storm.utils.MqttSensors;
import org.sense.storm.utils.TagSite;

public class MqttSensorExpensiveShuffleTopology {

	private static final Logger logger = Logger.getLogger(MqttSensorExpensiveShuffleTopology.class);
	private static final String BOLT_SENSOR_PRINT = "bolt-sensor-print";

	public MqttSensorExpensiveShuffleTopology(String msg, String ipAddressSource01) throws Exception {
		logger.info("Topology with AGGREGATE over a window, Site Aware Schedule and Metrics selected.");

		// Create Config instance for cluster configuration
		Config config = new Config();
		config.setDebug(false);
		// set the number of Workers on each node. This is not parallelism of tasks yet.
		config.setNumWorkers(2);

		// Data stream topology
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		// @formatter:off
		// Fields
		Fields fields = new Fields(MqttSensors.FIELD_SENSOR_ID.getValue(), 
				MqttSensors.FIELD_SENSOR_TYPE.getValue(), 
				MqttSensors.FIELD_PLATFORM_ID.getValue(), 
				MqttSensors.FIELD_PLATFORM_TYPE.getValue(),
				MqttSensors.FIELD_STATION_ID.getValue(), 
				MqttSensors.FIELD_TIMESTAMP.getValue(), 
				MqttSensors.FIELD_VALUE.getValue());

		// Spouts: data stream from count ticket and count train sensors
		// Spouts configured to run 1 EXECUTOR and 1 TASK for each EXECUTOR.
		topologyBuilder.setSpout(MqttSensors.SPOUT_STATION_01.getValue(), 
				new MqttSensorDetailSpout(ipAddressSource01, MqttSensors.TOPIC_STATION_01.getValue(), fields), 1)
				.addConfiguration(TagSite.SITE.getValue(), TagSite.CLUSTER.getValue())
				;
		topologyBuilder.setSpout(MqttSensors.SPOUT_STATION_02.getValue(), 
				new MqttSensorDetailSpout(ipAddressSource01, MqttSensors.TOPIC_STATION_02.getValue(), fields), 1)
				.addConfiguration(TagSite.SITE.getValue(), TagSite.CLUSTER.getValue())
				;

		// Bolts to partition tuples by key. The key is the sensor type.
		// Bolts configured to run 2 EXECUTORS and 4 TASKS (2 TASKS for each EXECUTOR).
		topologyBuilder.setBolt(MqttSensors.BOLT_SENSOR_TYPE.getValue(), new SensorAggregateValuesWindowBolt().withTumblingWindow(Duration.seconds(5)), 2)
				.fieldsGrouping(MqttSensors.SPOUT_STATION_01.getValue(), new Fields(MqttSensors.FIELD_SENSOR_TYPE.getValue()))
				.fieldsGrouping(MqttSensors.SPOUT_STATION_02.getValue(), new Fields(MqttSensors.FIELD_SENSOR_TYPE.getValue()))
				.setNumTasks(4) // This will create 4 Bolt instances 
				.addConfiguration(TagSite.SITE.getValue(), TagSite.CLUSTER.getValue())
				;

		// Printer Bolt
		topologyBuilder.setBolt(BOLT_SENSOR_PRINT, new SensorPrinterBolt(3), 2)
				.shuffleGrouping(MqttSensors.BOLT_SENSOR_TYPE.getValue())
				.addConfiguration(TagSite.SITE.getValue(), TagSite.CLUSTER.getValue())
				;
		// @formatter:on

		if (msg != null && msg.equalsIgnoreCase("CLUSTER")) {
			logger.info("Running on the cluster");
			StormSubmitter.submitTopologyWithProgressBar("MqttSensorExpensiveShuffleTopology", config,
					topologyBuilder.createTopology());
		} else {
			logger.info("Running on local machine");
			// execute the topology
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("MqttSensorExpensiveShuffleTopology", config, topologyBuilder.createTopology());
			Thread.sleep(60000);
			// Stop the topology
			cluster.shutdown();
		}
	}
}
