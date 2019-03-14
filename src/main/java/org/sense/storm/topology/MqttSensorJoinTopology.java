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

public class MqttSensorJoinTopology {

	final static Logger logger = Logger.getLogger(MqttSensorJoinTopology.class);

	private static final String TOPIC_STATION_01_PEOPLE = "topic-station-01-people";
	private static final String TOPIC_STATION_01_TRAINS = "topic-station-01-trains";
	private static final String TOPIC_STATION_01_TICKETS = "topic-station-01-tickets";
	private static final String TOPIC_STATION_02_PEOPLE = "topic-station-02-people";
	private static final String TOPIC_STATION_02_TRAINS = "topic-station-02-trains";
	private static final String TOPIC_STATION_02_TICKETS = "topic-station-02-tickets";

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
		Fields fields = new Fields(MqttSensorDetailSpout.FIELD_SENSOR_ID, 
				MqttSensorDetailSpout.FIELD_SENSOR_TYPE, 
				MqttSensorDetailSpout.FIELD_PLATFORM_ID, 
				MqttSensorDetailSpout.FIELD_PLATFORM_TYPE,
				MqttSensorDetailSpout.FIELD_STATION_ID, 
				MqttSensorDetailSpout.FIELD_VALUE);

		// Spouts: data stream from count ticket and count train sensors
		builder.setSpout(MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS, new MqttSensorDetailSpout(TOPIC_STATION_01_TICKETS, fields));
		builder.setSpout(MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS, new MqttSensorDetailSpout(TOPIC_STATION_01_TRAINS, fields));

		// Joiner Bolt
		SensorJoinTicketTrainPrinterBolt projection = new SensorJoinTicketTrainPrinterBolt(1);
		JoinBolt joiner = new JoinBolt(MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS, MqttSensorDetailSpout.FIELD_PLATFORM_ID)
				.join(MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS, MqttSensorDetailSpout.FIELD_PLATFORM_ID, MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS)
				.select(projection.getProjection())
				.withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));
		builder.setBolt(BOLT_SENSOR_JOINER, joiner)
				.fieldsGrouping(MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS, new Fields(MqttSensorDetailSpout.FIELD_PLATFORM_ID))
				.fieldsGrouping(MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS, new Fields(MqttSensorDetailSpout.FIELD_PLATFORM_ID));

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
