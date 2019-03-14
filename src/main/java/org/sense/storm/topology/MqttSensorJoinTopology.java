package org.sense.storm.topology;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.sense.storm.bolt.SensorJoinTicketTrainPrinterBolt;
import org.sense.storm.spout.MqttSensorDetailSpout;
import org.sense.storm.utils.MqttSensors;

public class MqttSensorJoinTopology {

	final static Logger logger = Logger.getLogger(MqttSensorJoinTopology.class);

	private static final String BOLT_SENSOR_JOINER = "bolt-sensor-joiner";
	private static final String BOLT_SENSOR_PRINT = "bolt-sensor-print";

	public MqttSensorJoinTopology(String msg) throws Exception {
		logger.info("Topology with JOIN and WINDOW selected.");
		// Create Config instance for cluster configuration
		Config config = new Config();
		config.setDebug(false);

		TopologyBuilder builder = new TopologyBuilder();

		// @formatter:off
		// Fields
		Fields fields = new Fields(MqttSensors.FIELD_SENSOR_ID.getValue(), 
				MqttSensors.FIELD_SENSOR_TYPE.getValue(), 
				MqttSensors.FIELD_PLATFORM_ID.getValue(), 
				MqttSensors.FIELD_PLATFORM_TYPE.getValue(),
				MqttSensors.FIELD_STATION_ID.getValue(), 
				MqttSensors.FIELD_VALUE.getValue());

		
		// Spouts: data stream from count ticket and count train sensors
		builder.setSpout(MqttSensors.SPOUT_STATION_01_TICKETS.getValue(), new MqttSensorDetailSpout(MqttSensors.TOPIC_STATION_01_TICKETS.getValue(), fields));
		builder.setSpout(MqttSensors.SPOUT_STATION_01_TRAINS.getValue(), new MqttSensorDetailSpout(MqttSensors.TOPIC_STATION_01_TRAINS.getValue(), fields));

		// Joiner Bolt
		SensorJoinTicketTrainPrinterBolt projection = new SensorJoinTicketTrainPrinterBolt(1);
		JoinBolt joiner = new JoinBolt(MqttSensors.SPOUT_STATION_01_TICKETS.getValue(), MqttSensors.FIELD_PLATFORM_ID.getValue())
				.join(MqttSensors.SPOUT_STATION_01_TRAINS.getValue(), MqttSensors.FIELD_PLATFORM_ID.getValue(), MqttSensors.SPOUT_STATION_01_TICKETS.getValue())
				.select(projection.getProjection())
				.withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));
		builder.setBolt(BOLT_SENSOR_JOINER, joiner)
				.fieldsGrouping(MqttSensors.SPOUT_STATION_01_TICKETS.getValue(), new Fields(MqttSensors.FIELD_PLATFORM_ID.getValue()))
				.fieldsGrouping(MqttSensors.SPOUT_STATION_01_TRAINS.getValue(), new Fields(MqttSensors.FIELD_PLATFORM_ID.getValue()));

		// Printer Bolt
		builder.setBolt(BOLT_SENSOR_PRINT, projection).shuffleGrouping(BOLT_SENSOR_JOINER);
		// @formatter:off

		if (msg != null && msg.equalsIgnoreCase("CLUSTER")) {
			logger.info("Running on the cluster");
			config.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar("MqttSensorJoinTopology", config, builder.createTopology());
		} else {
			logger.info("Running on local machine");
			// execute the topology
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("MqttSensorJoinTopology", config, builder.createTopology());
			Thread.sleep(60000);

			// Stop the topology
			cluster.shutdown();
		}
	}
}
