package org.sense.storm.bolt;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class SensorPrinterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 7146201246991885765L;
	private static final Logger logger = Logger.getLogger(SensorPrinterBolt.class);
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private Integer id;
	private String name;
	private OutputCollector collector;
	private List<String> result;
	private int projectionId;

	public SensorPrinterBolt(int projectionId) {
		this.projectionId = projectionId;
		// this.setProjection(projectionId);
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.result = new ArrayList<String>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	public void execute(Tuple input) {
		Integer sensorId = null;
		String sensorType = null;
		Integer platformId = null;
		String platformType = null;
		Integer stationId = null;
		Long timestamp = null;
		Double value = null;

		Integer secondSensorId = null;
		String secondSensorType = null;
		Integer secondPlatformId = null;
		String secondPlatformType = null;
		Integer secondStationId = null;
		Long secondTimestamp = null;
		Double secondValue = null;

		String resultToPrint = "";

		switch (projectionId) {
		case 0:
			try {
				sensorId = input.getInteger(0);
			} catch (NumberFormatException re) {
				System.err.println("Error converting sensorId.");
			}
			try {
				sensorType = input.getString(1);
			} catch (ClassCastException re) {
				System.err.println("Error converting sensorType.");
			}
			try {
				platformId = input.getInteger(2);
			} catch (NumberFormatException re) {
				System.err.println("Error converting platformId.");
			}
			try {
				platformType = input.getString(3);
			} catch (ClassCastException re) {
				System.err.println("Error converting platformType.");
			}
			try {
				stationId = input.getInteger(4);
			} catch (NumberFormatException re) {
				System.err.println("Error converting stationId.");
			}
			try {
				timestamp = input.getLong(5);
			} catch (NumberFormatException re) {
				System.err.println("Error converting timestamp.");
			}
			try {
				value = input.getDouble(6);
			} catch (NumberFormatException re) {
				System.err.println("Error converting value.");
			}
			resultToPrint = "sensorId[" + sensorId + "] sensorType[" + sensorType + "] platformId[" + platformId
					+ "] platformType[" + platformType + "] stationId[" + stationId + "] timestamp["
					+ sdf.format(new Date(timestamp)) + "] value[" + value + "]";
			break;
		case 1:
			// TICKETS: sensorId, sensorType, timestamp, value
			try {
				sensorId = input.getInteger(0);
			} catch (NumberFormatException re) {
				System.err.println("Error converting sensorId.");
			}
			try {
				sensorType = input.getString(1);
			} catch (ClassCastException re) {
				System.err.println("Error converting sensorType.");
			}
			try {
				timestamp = input.getLong(2);
			} catch (NumberFormatException re) {
				System.err.println("Error converting timestamp.");
			}
			try {
				value = input.getDouble(3);
			} catch (NumberFormatException re) {
				System.err.println("Error converting value.");
			}
			// TRAINS: sensorId, sensorType, timestamp, value
			try {
				secondSensorId = input.getInteger(4);
			} catch (NumberFormatException re) {
				System.err.println("Error converting sensorId.");
			}
			try {
				secondSensorType = input.getString(5);
			} catch (ClassCastException re) {
				System.err.println("Error converting sensorType.");
			}
			try {
				secondTimestamp = input.getLong(6);
			} catch (NumberFormatException re) {
				System.err.println("Error converting timestamp.");
			}
			try {
				secondValue = input.getDouble(7);
			} catch (NumberFormatException re) {
				System.err.println("Error converting value.");
			}
			// COMMON VALUES: platformType, stationId
			try {
				platformType = input.getString(8);
			} catch (ClassCastException re) {
				System.err.println("Error converting platformType.");
			}
			try {
				stationId = input.getInteger(9);
			} catch (NumberFormatException re) {
				System.err.println("Error converting stationId.");
			}
			resultToPrint = "First     -> sensorId[" + sensorId + "] sensorType[" + sensorType + "] timestamp["
					+ sdf.format(new Date(timestamp)) + "] value[" + value + "]\nSecond    -> sensorId["
					+ secondSensorId + "] sensorType[" + secondSensorType + "] timestamp["
					+ sdf.format(new Date(secondTimestamp)) + "] value[" + secondValue + "]\n             platformType["
					+ platformType + "] stationId[" + stationId + "]";
			break;
		case 2:
			// TICKETS: sensorType, platformId, timestamp, sum
			try {
				sensorType = input.getString(0);
			} catch (ClassCastException re) {
				System.err.println("Error converting sensorType.");
			}
			try {
				platformId = input.getInteger(1);
			} catch (NumberFormatException re) {
				System.err.println("Error converting platformId.");
			}
			try {
				timestamp = input.getLong(2);
			} catch (NumberFormatException re) {
				System.err.println("Error converting timestamp.");
			}
			try {
				value = input.getDouble(3);
			} catch (NumberFormatException re) {
				System.err.println("Error converting value.");
			}
			// TRAINS: sensorType, platformId, timestamp, sum
			try {
				secondSensorType = input.getString(4);
			} catch (ClassCastException re) {
				System.err.println("Error converting sensorType.");
			}
			try {
				secondPlatformId = input.getInteger(5);
			} catch (NumberFormatException re) {
				System.err.println("Error converting platformId.");
			}
			try {
				secondTimestamp = input.getLong(6);
			} catch (NumberFormatException re) {
				System.err.println("Error converting timestamp.");
			}
			try {
				secondValue = input.getDouble(7);
			} catch (NumberFormatException re) {
				System.err.println("Error converting value.");
			}
			resultToPrint = "First    -> sensorType[" + sensorType + "] platformId[" + platformId + "] timestamp["
					+ sdf.format(new Date(timestamp)) + "] value[" + value + "] " + "\nSecond   -> sensorType["
					+ secondSensorType + "] platformId[" + secondPlatformId + "] timestamp["
					+ sdf.format(new Date(secondTimestamp)) + "] value[" + secondValue + "]";
			break;
		default:
			try {
				sensorId = input.getInteger(0);
			} catch (NumberFormatException re) {
				System.err.println("Error converting sensorId.");
			}
			try {
				sensorType = input.getString(1);
			} catch (ClassCastException re) {
				System.err.println("Error converting sensorType.");
			}
			try {
				platformId = input.getInteger(2);
			} catch (NumberFormatException re) {
				System.err.println("Error converting platformId.");
			}
			try {
				platformType = input.getString(3);
			} catch (ClassCastException re) {
				System.err.println("Error converting platformType.");
			}
			try {
				stationId = input.getInteger(4);
			} catch (NumberFormatException re) {
				System.err.println("Error converting stationId.");
			}
			try {
				timestamp = input.getLong(5);
			} catch (NumberFormatException re) {
				System.err.println("Error converting timestamp.");
			}
			try {
				value = input.getDouble(6);
			} catch (NumberFormatException re) {
				System.err.println("Error converting value.");
			}
			resultToPrint = "sensorId[" + sensorId + "] sensorType[" + sensorType + "] platformId[" + platformId
					+ "] platformType[" + platformType + "] stationId[" + stationId + "] timestamp["
					+ sdf.format(new Date(timestamp)) + "] value[" + value + "]";
			break;
		}

		result.add(input.toString());

		collector.ack(input);
		// print here or wait until the application finishes to execute the cleanup()
		// method
		// logger.info(resultToPrint);
		System.out.println(resultToPrint);
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
}
