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
import org.sense.storm.bolt.SensorJoinTicketTrainPrinterBolt;
import org.sense.storm.bolt.SumSensorValuesWindowBolt;
import org.sense.storm.spout.MqttSensorDetailSpout;
import org.sense.storm.utils.MqttSensors;
import org.sense.storm.utils.SensorType;
import org.sense.storm.utils.TagSite;

import com.github.staslev.storm.metrics.MetricReporter;
import com.github.staslev.storm.metrics.MetricReporterConfig;
import com.github.staslev.storm.metrics.yammer.SimpleGraphiteStormMetricProcessor;
import com.github.staslev.storm.metrics.yammer.YammerFacadeMetric;

public class MqttSensorSumTopology {

	final static Logger logger = Logger.getLogger(MqttSensorSumTopology.class);

	private static final String BOLT_SENSOR_JOINER = "bolt-sensor-joiner";
	private static final String BOLT_SENSOR_PRINT = "bolt-sensor-print";

	public MqttSensorSumTopology(String msg, String ipAddressSource01) throws Exception {
		logger.info(
				"Topology with AGGREGATE over a window, JOIN and Default Resource Aware Schedule and Metrics selected.");

		// Create Config instance for cluster configuration
		Config config = new Config();
		config.setDebug(false);
		// number of Workers on each node. This is for task parallelism.
		config.setNumWorkers(2);

		// The memory limit a worker process will be allocated in megabytes
		// config.setTopologyWorkerMaxHeapSize(512.0);

		// Profiling Resource Usage: Log all storm metrics
		config.put(YammerFacadeMetric.FACADE_METRIC_TIME_BUCKET_IN_SEC, 30);
		config.put(SimpleGraphiteStormMetricProcessor.GRAPHITE_HOST, "192.168.56.1");
		config.put(SimpleGraphiteStormMetricProcessor.GRAPHITE_PORT, 2003);
		config.put(SimpleGraphiteStormMetricProcessor.REPORT_PERIOD_IN_SEC, 10);
		config.put(Config.TOPOLOGY_NAME, MqttSensorSumTopology.class.getCanonicalName());
		config.registerMetricsConsumer(MetricReporter.class,
				new MetricReporterConfig(".*", SimpleGraphiteStormMetricProcessor.class.getCanonicalName()), 1);

		// Data stream topology
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		// @formatter:off
		// Fields
		Fields fields = new Fields(MqttSensors.FIELD_SENSOR_ID.getValue(), 
				MqttSensors.FIELD_SENSOR_TYPE.getValue(), 
				MqttSensors.FIELD_PLATFORM_ID.getValue(), 
				MqttSensors.FIELD_PLATFORM_TYPE.getValue(),
				MqttSensors.FIELD_STATION_ID.getValue(), 
				MqttSensors.FIELD_VALUE.getValue());

		// Spouts: data stream from count ticket and count train sensors
		topologyBuilder.setSpout(MqttSensors.SPOUT_STATION_01_TICKETS.getValue(), new MqttSensorDetailSpout(ipAddressSource01, MqttSensors.TOPIC_STATION_01_TICKETS.getValue(), fields))
				.addConfiguration(TagSite.SITE.getValue(), TagSite.EDGE.getValue());
		topologyBuilder.setSpout(MqttSensors.SPOUT_STATION_01_TRAINS.getValue(), new MqttSensorDetailSpout(ipAddressSource01, MqttSensors.TOPIC_STATION_01_TRAINS.getValue(), fields))
				.addConfiguration(TagSite.SITE.getValue(), TagSite.EDGE.getValue());

		// Bolts to compute the sum of TICKETS and TRAINS
		topologyBuilder.setBolt(MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue(), new SumSensorValuesWindowBolt(SensorType.COUNTER_TICKETS).withTumblingWindow(Duration.seconds(5)), 1)
				.shuffleGrouping(MqttSensors.SPOUT_STATION_01_TICKETS.getValue())
				.addConfiguration(TagSite.SITE.getValue(), TagSite.EDGE.getValue());
		topologyBuilder.setBolt(MqttSensors.BOLT_SENSOR_TRAIN_SUM.getValue(), new SumSensorValuesWindowBolt(SensorType.COUNTER_TRAINS).withTumblingWindow(Duration.seconds(5)), 1)
				.shuffleGrouping(MqttSensors.SPOUT_STATION_01_TRAINS.getValue())
				.addConfiguration(TagSite.SITE.getValue(), TagSite.EDGE.getValue());

		// Joiner Bolt
		SensorJoinTicketTrainPrinterBolt projection = new SensorJoinTicketTrainPrinterBolt(2);
		JoinBolt joiner = new JoinBolt(MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue(), MqttSensors.FIELD_PLATFORM_ID.getValue())
		 		.join(MqttSensors.BOLT_SENSOR_TRAIN_SUM.getValue(), MqttSensors.FIELD_PLATFORM_ID.getValue(), MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue())
		 		.select(projection.getProjection())
		 		.withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));
		topologyBuilder.setBolt(BOLT_SENSOR_JOINER, joiner)
				.fieldsGrouping(MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue(), new Fields(MqttSensors.FIELD_PLATFORM_ID.getValue()))
				.fieldsGrouping(MqttSensors.BOLT_SENSOR_TRAIN_SUM.getValue(), new Fields(MqttSensors.FIELD_PLATFORM_ID.getValue()))
				.addConfiguration(TagSite.SITE.getValue(), TagSite.CLUSTER.getValue());

		// Printer Bolt
		topologyBuilder.setBolt(BOLT_SENSOR_PRINT, projection).shuffleGrouping(BOLT_SENSOR_JOINER)
				.addConfiguration(TagSite.SITE.getValue(), TagSite.CLUSTER.getValue());
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
