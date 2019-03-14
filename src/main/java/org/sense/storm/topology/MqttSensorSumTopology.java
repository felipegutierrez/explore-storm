package org.sense.storm.topology;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.sense.storm.bolt.PrinterBolt;
import org.sense.storm.bolt.SensorJoinTicketTrainPrinterBolt;
import org.sense.storm.bolt.SumSensorValuesWindowBolt;
import org.sense.storm.spout.MqttSensorDetailSpout;
import org.sense.storm.utils.MqttSensors;
import org.sense.storm.utils.SensorType;

public class MqttSensorSumTopology {

	final static Logger logger = Logger.getLogger(MqttSensorSumTopology.class);

	private static final String BOLT_SENSOR_TICKET_SUM = "bolt-sensor-ticket-sum";
	private static final String BOLT_SENSOR_TRAIN_SUM = "bolt-sensor-train-sum";
	private static final String BOLT_SENSOR_PRINT = "bolt-sensor-print";

	public MqttSensorSumTopology(String msg) throws Exception {
		logger.info(
				"Topology with AGGREGATE over a window, JOIN and Default Resource Aware Schedule and Metrics selected.");

		// Create Config instance for cluster configuration
		Config config = new Config();
		config.setDebug(false);
		config.setNumWorkers(1);

		// The memory limit a worker process will be allocated in megabytes
		// config.setTopologyWorkerMaxHeapSize(512.0);

		// Profiling Resource Usage: Log all storm metrics
		// This piece of code is commented because it is throwing the error:
		// java.lang.UnsatisfiedLinkError: org.hyperic.sigar.Sigar.getPid()
		config.registerMetricsConsumer(LoggingMetricsConsumer.class);
		Map<String, String> workerMetrics = new HashMap<String, String>();
		workerMetrics.put("CPU", "org.apache.storm.metrics.sigar.CPUMetric");
		config.put(Config.TOPOLOGY_WORKER_METRICS, workerMetrics);

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
		topologyBuilder.setSpout(MqttSensors.SPOUT_STATION_01_TICKETS.getValue(), new MqttSensorDetailSpout(MqttSensors.TOPIC_STATION_01_TICKETS.getValue(), fields))
				// .setMemoryLoad(512.0)
				// .setCPULoad(100.0)
				;
		topologyBuilder.setSpout(MqttSensors.SPOUT_STATION_01_TRAINS.getValue(), new MqttSensorDetailSpout(MqttSensors.TOPIC_STATION_01_TRAINS.getValue(), fields))
				// .setMemoryLoad(512.0)
				// .setCPULoad(100.0)
				;

		// Bolts to compute the sum of TICKETS and TRAINS
		topologyBuilder.setBolt(BOLT_SENSOR_TICKET_SUM, new SumSensorValuesWindowBolt(SensorType.COUNTER_TICKETS).withTumblingWindow(Duration.seconds(5)), 1)
				.shuffleGrouping(MqttSensors.SPOUT_STATION_01_TICKETS.getValue())
				// .setMemoryLoad(512.0)
				// .setCPULoad(10.0)
				;
		topologyBuilder.setBolt(BOLT_SENSOR_TRAIN_SUM, new SumSensorValuesWindowBolt(SensorType.COUNTER_TRAINS).withTumblingWindow(Duration.seconds(5)), 1)
				.shuffleGrouping(MqttSensors.SPOUT_STATION_01_TRAINS.getValue())
				// .setMemoryLoad(512.0)
				// .setCPULoad(10.0)
				;

		// Joiner Bolt
		SensorJoinTicketTrainPrinterBolt projection = new SensorJoinTicketTrainPrinterBolt(1);
		// JoinBolt joiner = new JoinBolt(BOLT_SENSOR_TICKET_SUM, MqttSensorDetailSpout.FIELD_PLATFORM_ID)
		// 		.join(BOLT_SENSOR_TRAIN_SUM, MqttSensorDetailSpout.FIELD_PLATFORM_ID, MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS)
		// 		.select(projection.getProjection())
		// 		.withTumblingWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));
		
		
		// Printer Bolt
		topologyBuilder.setBolt(BOLT_SENSOR_PRINT, new PrinterBolt(), 1)
				.shuffleGrouping(BOLT_SENSOR_TICKET_SUM)
				.shuffleGrouping(BOLT_SENSOR_TRAIN_SUM)
				// .setMemoryLoad(512.0)
				// .setCPULoad(10.0)
				;
		// @formatter:on

		if (msg != null && msg.equalsIgnoreCase("CLUSTER")) {
			logger.info("Running on the cluster");
			config.setNumWorkers(1);
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
