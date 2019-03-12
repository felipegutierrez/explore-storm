package org.sense.storm.topology;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.sense.storm.bolt.PrinterBolt;
import org.sense.storm.bolt.SumTicketWindowBolt;
import org.sense.storm.spout.MqttSensorDetailSpout;

public class MqttSensorSumTopology {

	final static Logger logger = Logger.getLogger(MqttSensorSumTopology.class);

	private static final String TOPIC_STATION_01_PEOPLE = "topic-station-01-people";
	private static final String TOPIC_STATION_01_TRAINS = "topic-station-01-trains";
	private static final String TOPIC_STATION_01_TICKETS = "topic-station-01-tickets";
	private static final String TOPIC_STATION_02_PEOPLE = "topic-station-02-people";
	private static final String TOPIC_STATION_02_TRAINS = "topic-station-02-trains";
	private static final String TOPIC_STATION_02_TICKETS = "topic-station-02-tickets";

	private static final String BOLT_SENSOR_TICKET_SUM = "bolt-sensor-ticket-sum";
	private static final String BOLT_SENSOR_PRINT = "bolt-sensor-print";

	public MqttSensorSumTopology(String msg) throws Exception {
		// Create Config instance for cluster configuration
		Config config = new Config();
		config.setDebug(false);
		config.setNumWorkers(1);

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
		// builder.setSpout(MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS, new MqttSensorDetailSpout(TOPIC_STATION_01_TRAINS, fields));

		// @formatter:on
		builder.setBolt(BOLT_SENSOR_TICKET_SUM, new SumTicketWindowBolt().withTumblingWindow(Duration.seconds(5)), 1)
				.shuffleGrouping(MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS);

		// Printer Bolt
		builder.setBolt(BOLT_SENSOR_PRINT, new PrinterBolt(), 1).shuffleGrouping(BOLT_SENSOR_TICKET_SUM);

		if (msg != null && msg.equalsIgnoreCase("CLUSTER")) {
			logger.info("Running on the cluster");
			config.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar("MqttSensorSumTopology", config, builder.createTopology());
		} else {
			logger.info("Running on local machine");
			// execute the topology
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("MqttSensorSumTopology", config, builder.createTopology());
			Thread.sleep(60000);

			// Stop the topology
			cluster.shutdown();
		}
	}
}
