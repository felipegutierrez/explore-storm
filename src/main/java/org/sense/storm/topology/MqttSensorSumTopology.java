package org.sense.storm.topology;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.sense.storm.bolt.SensorJoinTicketTrainBolt;
import org.sense.storm.bolt.SensorPrinterBolt;
import org.sense.storm.bolt.SumSensorValuesWindowBolt;
import org.sense.storm.spout.MqttSensorDetailSpout;
import org.sense.storm.utils.MqttSensors;
import org.sense.storm.utils.SensorType;
import org.sense.storm.utils.TagSite;

public class MqttSensorSumTopology {

	private static final Logger logger = Logger.getLogger(MqttSensorSumTopology.class);
	private static final String BOLT_SENSOR_JOINER = "bolt-sensor-joiner";
	private static final String BOLT_SENSOR_PRINT = "bolt-sensor-print";

	public MqttSensorSumTopology(String msg, String ipAddressSource01) throws Exception {
		logger.info(
				"Topology with AGGREGATE over a window, JOIN and Default Resource Aware Schedule and Metrics selected.");

		// Create Config instance for cluster configuration
		Config config = new Config();
		config.setDebug(false);
		// set the number of Workers on each node. This is not parallelism of tasks yet.
		config.setNumWorkers(2);

		// The memory limit a worker process will be allocated in megabytes
		// config.setTopologyWorkerMaxHeapSize(512.0);

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
		// Spouts configured to run 2 EXECUTORS in parallel but only 1 TASK.
		topologyBuilder.setSpout(MqttSensors.SPOUT_STATION_01_TICKETS.getValue(), 
				new MqttSensorDetailSpout(ipAddressSource01, MqttSensors.TOPIC_STATION_01_TICKETS.getValue(), fields), 2)
				.setNumTasks(1)
				.addConfiguration(TagSite.SITE.getValue(), TagSite.EDGE.getValue())
				;
		topologyBuilder.setSpout(MqttSensors.SPOUT_STATION_01_TRAINS.getValue(), 
				new MqttSensorDetailSpout(ipAddressSource01, MqttSensors.TOPIC_STATION_01_TRAINS.getValue(), fields), 2)
				.setNumTasks(1)
				.addConfiguration(TagSite.SITE.getValue(), TagSite.EDGE.getValue())
				;

		// Bolts to compute the sum of TICKETS and TRAINS
		// Bolts configured to run 2 EXECUTORS in parallel and 2 TASKS each. There will be 4 TASKS instances in total.
		topologyBuilder.setBolt(MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue(), 
				new SumSensorValuesWindowBolt(SensorType.COUNTER_TICKETS).withTumblingWindow(Duration.seconds(5)), 2)
				.setNumTasks(2)
				.shuffleGrouping(MqttSensors.SPOUT_STATION_01_TICKETS.getValue())
				.addConfiguration(TagSite.SITE.getValue(), TagSite.EDGE.getValue())
				;
		topologyBuilder.setBolt(MqttSensors.BOLT_SENSOR_TRAIN_SUM.getValue(), 
				new SumSensorValuesWindowBolt(SensorType.COUNTER_TRAINS).withTumblingWindow(Duration.seconds(5)), 2)
				.setNumTasks(2)
				.shuffleGrouping(MqttSensors.SPOUT_STATION_01_TRAINS.getValue())
				.addConfiguration(TagSite.SITE.getValue(), TagSite.EDGE.getValue())
				;

		// Joiner Bolt
		int projectionId = 2;
		SensorJoinTicketTrainBolt projection = new SensorJoinTicketTrainBolt(projectionId);
		JoinBolt joiner = new JoinBolt(MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue(), MqttSensors.FIELD_PLATFORM_ID.getValue())
		 		.join(MqttSensors.BOLT_SENSOR_TRAIN_SUM.getValue(), MqttSensors.FIELD_PLATFORM_ID.getValue(), MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue())
		 		.select(projection.getProjection())
		 		.withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));
		topologyBuilder.setBolt(BOLT_SENSOR_JOINER, joiner, 2)
				.setNumTasks(2)
				.fieldsGrouping(MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue(), new Fields(MqttSensors.FIELD_PLATFORM_ID.getValue()))
				.fieldsGrouping(MqttSensors.BOLT_SENSOR_TRAIN_SUM.getValue(), new Fields(MqttSensors.FIELD_PLATFORM_ID.getValue()))
				.addConfiguration(TagSite.SITE.getValue(), TagSite.CLUSTER.getValue())
				;

		// Printer Bolt
		topologyBuilder.setBolt(BOLT_SENSOR_PRINT, new SensorPrinterBolt(projectionId), 2)
				.setNumTasks(2)
				.shuffleGrouping(BOLT_SENSOR_JOINER)
				.addConfiguration(TagSite.SITE.getValue(), TagSite.CLUSTER.getValue())
				;
		// @formatter:on

		if (msg != null && msg.equalsIgnoreCase("CLUSTER")) {
			logger.info("Running on the cluster");
			StormSubmitter.submitTopologyWithProgressBar("MqttSensorSumTopology", config,
					topologyBuilder.createTopology());
		} else {
			logger.info("Running on local machine");
			// execute the topology
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("MqttSensorSumTopology", config, topologyBuilder.createTopology());
			Thread.sleep(60000);
			// Stop the topology
			cluster.shutdown();
		}
	}
}
