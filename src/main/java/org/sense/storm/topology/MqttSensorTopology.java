package org.sense.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.sense.storm.bolt.SensorMapperBolt;
import org.sense.storm.bolt.SensorPrintBolt;
import org.sense.storm.spout.MqttSensorSpout;

public class MqttSensorTopology {

	private static final String SPOUT_STATION_01 = "spout-station-01";
	private static final String SPOUT_STATION_02 = "spout-station-02";
	private static final String BOLT_CREATE_SENSOR = "bolt-create-sensor-tuple";
	private static final String BOLT_SENSOR_PRINT = "bolt-sensor-print";

	public MqttSensorTopology(String msg) throws Exception {
		// Create Config instance for cluster configuration
		Config config = new Config();
		config.setDebug(false);

		TopologyBuilder builder = new TopologyBuilder();

		// @formatter:off
		// Spouts
		builder.setSpout(SPOUT_STATION_01, new MqttSensorSpout("topic-station-01"))
				.addConfiguration("tags", "GPU");
		builder.setSpout(SPOUT_STATION_02, new MqttSensorSpout("topic-station-02"))
				.addConfiguration("tags", "GPU");

		// Bolts
		builder.setBolt(BOLT_CREATE_SENSOR, new SensorMapperBolt())
				.shuffleGrouping(SPOUT_STATION_01)
				// .shuffleGrouping(SPOUT_STATION_02)
				.addConfiguration("tags", "GPU");
		builder.setBolt(BOLT_SENSOR_PRINT, new SensorPrintBolt())
				.fieldsGrouping(BOLT_CREATE_SENSOR, new Fields("value"))
				.addConfiguration("tags", "GPU");
		// @formatter:on

		if (msg != null && msg.equalsIgnoreCase("CLUSTER")) {
			System.out.println("Running on the cluster");
			config.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar("MqttSensorAnalyserStorm", config, builder.createTopology());
		} else {
			System.out.println("Running on local machine");
			// execute the topology
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("MqttSensorAnalyserStorm", config, builder.createTopology());
			Thread.sleep(10000);

			// Stop the topology
			cluster.shutdown();
		}
	}
}
