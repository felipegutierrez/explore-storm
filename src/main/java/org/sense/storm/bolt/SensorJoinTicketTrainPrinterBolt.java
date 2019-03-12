package org.sense.storm.bolt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.sense.storm.spout.MqttSensorDetailSpout;

public class SensorJoinTicketTrainPrinterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 7146201246991885765L;

	final static Logger logger = Logger.getLogger(SensorJoinTicketTrainPrinterBolt.class);

	private String projection;
	private int projectionId;
	private Integer id;
	private String name;
	private OutputCollector collector;
	private List<String> result;

	public SensorJoinTicketTrainPrinterBolt(int projectionId) {
		this.projectionId = projectionId;
		this.setProjection(projectionId);
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.result = new ArrayList<String>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	public void execute(Tuple input) {
		Integer leftSensorId = null;
		String leftSensorType = null;
		Integer leftPlatformId = null;
		String leftPlatformType = null;
		Integer leftStationId = null;
		Double leftValue = null;

		Integer rightSensorId = null;
		String rightSensorType = null;
		Integer rightPlatformId = null;
		String rightPlatformType = null;
		Integer rightStationId = null;
		Double rightValue = null;

		String resultLeft = null;
		String resultRight = null;
		String resultProjection = null;

		switch (projectionId) {
		case 1:
			try {
				leftSensorId = input.getInteger(0);
			} catch (ClassCastException re) {
				System.err.println("Error converting left.sensorId.");
			}
			try {
				leftSensorType = input.getString(1);
			} catch (ClassCastException re) {
				System.err.println("Error converting left.sensorType.");
			}
			try {
				leftValue = input.getDouble(2);
			} catch (ClassCastException re) {
				System.err.println("Error converting left.value.");
			}

			try {
				rightSensorId = input.getInteger(3);
			} catch (ClassCastException re) {
				System.err.println("Error converting right.sensorId.");
			}
			try {
				rightSensorType = input.getString(4);
			} catch (ClassCastException re) {
				System.err.println("Error converting right.sensorType.");
			}
			try {
				rightValue = input.getDouble(5);
			} catch (ClassCastException re) {
				System.err.println("Error converting right.value.");
			}

			try {
				rightPlatformType = input.getString(6);
			} catch (ClassCastException re) {
				System.err.println("Error converting right.platformType.");
			}
			try {
				rightStationId = input.getInteger(7);
			} catch (ClassCastException re) {
				System.err.println("Error converting right.stationId.");
			}

			resultLeft = "left sensorId[" + leftSensorId + "] sensorType[" + leftSensorType + "] value[" + leftValue
					+ "]";
			resultRight = "right sensorId[" + rightSensorId + "] sensorType[" + rightSensorType + "] value["
					+ rightValue + "]";
			resultProjection = resultLeft + " - " + resultRight + " - platformType[" + rightPlatformType
					+ "] stationId[" + rightStationId + "]";
			break;
		default:
			try {
				leftSensorId = input.getInteger(0);
			} catch (ClassCastException re) {
				System.err.println("Error converting left.sensorId.");
			}
			try {
				leftSensorType = input.getString(1);
			} catch (ClassCastException re) {
				System.err.println("Error converting left.sensorType.");
			}
			try {
				leftPlatformId = input.getInteger(2);
			} catch (ClassCastException re) {
				System.err.println("Error converting left.platformId.");
			}
			try {
				leftPlatformType = input.getString(3);
			} catch (ClassCastException re) {
				System.err.println("Error converting left.platformType.");
			}
			try {
				leftStationId = input.getInteger(4);
			} catch (ClassCastException re) {
				System.err.println("Error converting left.stationId.");
			}
			try {
				leftValue = input.getDouble(5);
			} catch (ClassCastException re) {
				System.err.println("Error converting left.value.");
			}

			try {
				rightSensorId = input.getInteger(6);
			} catch (ClassCastException re) {
				System.err.println("Error converting right.sensorId.");
			}
			try {
				rightSensorType = input.getString(7);
			} catch (ClassCastException re) {
				System.err.println("Error converting right.sensorType.");
			}
			try {
				rightPlatformId = input.getInteger(8);
			} catch (ClassCastException re) {
				System.err.println("Error converting right.platformId.");
			}
			try {
				rightPlatformType = input.getString(9);
			} catch (ClassCastException re) {
				System.err.println("Error converting right.platformType.");
			}
			try {
				rightStationId = input.getInteger(10);
			} catch (ClassCastException re) {
				System.err.println("Error converting right.stationId.");
			}
			try {
				rightValue = input.getDouble(11);
			} catch (ClassCastException re) {
				System.err.println("Error converting right.value.");
			}
			resultLeft = "left sensorId[" + leftSensorId + "] sensorType[" + leftSensorType + "] platformId["
					+ leftPlatformId + "] platformType[" + leftPlatformType + "] stationId[" + leftStationId
					+ "] value[" + leftValue + "]";

			resultRight = "right sensorId[" + rightSensorId + "] sensorType[" + rightSensorType + "] platformId["
					+ rightPlatformId + "] platformType[" + rightPlatformType + "] stationId[" + rightStationId
					+ "] value[" + rightValue + "]";
			resultProjection = resultLeft + " - " + resultRight;
			break;
		}

		result.add(input.toString());

		collector.ack(input);
		// print here or wait until the application finishes to execute the cleanup()
		// method
		logger.info(resultProjection);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declare(new Fields("value"));
	}

	/**
	 * This method may be called when the Bolt shuts down. However there is no
	 * guarantee that the method will be called and it will only be called if Storm
	 * is run using debug mode in a local cluster. We only use it here to print the
	 * results of the word count when we have finished streaming our file and will
	 * remove it later.
	 */
	@Override
	public void cleanup() {
		logger.info("-- Word Counter [" + name + "-" + id + "] --");
		for (Iterator<String> iterator = result.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			logger.info(string);
		}
	}

	public String getProjection() {
		return projection;
	}

	private void setProjection(int id) {
		switch (id) {
		case 1:
			this.projection = MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS + ":"
					+ MqttSensorDetailSpout.FIELD_SENSOR_ID + "," + MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS + ":"
					+ MqttSensorDetailSpout.FIELD_SENSOR_TYPE + "," + MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS
					+ ":" + MqttSensorDetailSpout.FIELD_VALUE + "," + MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS
					+ ":" + MqttSensorDetailSpout.FIELD_SENSOR_ID + "," + MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS
					+ ":" + MqttSensorDetailSpout.FIELD_SENSOR_TYPE + ","
					+ MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS + ":" + MqttSensorDetailSpout.FIELD_VALUE + ","
					+ MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS + ":" + MqttSensorDetailSpout.FIELD_PLATFORM_TYPE
					+ "," + MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS + ":"
					+ MqttSensorDetailSpout.FIELD_STATION_ID;
			break;
		default:
			this.projection = MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS + ":"
					+ MqttSensorDetailSpout.FIELD_SENSOR_ID + "," + MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS + ":"
					+ MqttSensorDetailSpout.FIELD_SENSOR_TYPE + "," + MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS
					+ ":" + MqttSensorDetailSpout.FIELD_PLATFORM_ID + ","
					+ MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS + ":" + MqttSensorDetailSpout.FIELD_PLATFORM_TYPE
					+ "," + MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS + ":"
					+ MqttSensorDetailSpout.FIELD_STATION_ID + "," + MqttSensorDetailSpout.SPOUT_STATION_01_TICKETS
					+ ":" + MqttSensorDetailSpout.FIELD_VALUE + "," + MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS
					+ ":" + MqttSensorDetailSpout.FIELD_SENSOR_ID + "," + MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS
					+ ":" + MqttSensorDetailSpout.FIELD_SENSOR_TYPE + ","
					+ MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS + ":" + MqttSensorDetailSpout.FIELD_PLATFORM_ID
					+ "," + MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS + ":"
					+ MqttSensorDetailSpout.FIELD_PLATFORM_TYPE + "," + MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS
					+ ":" + MqttSensorDetailSpout.FIELD_STATION_ID + "," + MqttSensorDetailSpout.SPOUT_STATION_01_TRAINS
					+ ":" + MqttSensorDetailSpout.FIELD_VALUE;
			break;
		}
	}
}
