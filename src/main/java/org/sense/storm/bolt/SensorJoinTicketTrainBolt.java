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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.sense.storm.utils.MqttSensors;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

public class SensorJoinTicketTrainBolt extends BaseRichBolt {

	private static final long serialVersionUID = 7146201246991885765L;
	private static final Logger logger = Logger.getLogger(SensorJoinTicketTrainBolt.class);

	private String projection;
	private int projectionId;
	private Integer id;
	private String name;
	private OutputCollector collector;
	private List<String> result;

	private Meter tupleMeter;
	private Timer tupleTimer;
	private Histogram tupleHistogram;

	public SensorJoinTicketTrainBolt(int projectionId) {
		this.projectionId = projectionId;
		this.setProjection(projectionId);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.result = new ArrayList<String>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		this.tupleMeter = context.registerMeter("meterJoin-" + this.name);
		this.tupleTimer = context.registerTimer("timerJoin-" + this.name);
		this.tupleHistogram = context.registerHistogram("histogramJoin-" + this.name);
	}

	@Override
	public void execute(Tuple input) {
		final Timer.Context timeContext = this.tupleTimer.time();
		this.tupleMeter.mark();

		try {
			Integer leftSensorId = null;
			String leftSensorType = null;
			Integer leftPlatformId = null;
			String leftPlatformType = null;
			Integer leftStationId = null;
			Long leftTimestamp = null;
			Double leftValue = null;

			Integer rightSensorId = null;
			String rightSensorType = null;
			Integer rightPlatformId = null;
			String rightPlatformType = null;
			Integer rightStationId = null;
			Long rightTimestamp = null;
			Double rightValue = null;

			String resultLeft = null;
			String resultRight = null;
			String resultProjection = null;

			switch (projectionId) {
			case 1:
				// TICKETS: sensorId, sensorType, timestamp, value
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
					leftTimestamp = input.getLong(2);
				} catch (ClassCastException re) {
					System.err.println("Error converting left.timestamp.");
				}
				try {
					leftValue = input.getDouble(3);
				} catch (ClassCastException re) {
					System.err.println("Error converting left.value.");
				}
				// TRAINS: sensorId, sensorType, timestamp, value
				try {
					rightSensorId = input.getInteger(4);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.sensorId.");
				}
				try {
					rightSensorType = input.getString(5);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.sensorType.");
				}
				try {
					rightTimestamp = input.getLong(6);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.timestamp.");
				}
				try {
					rightValue = input.getDouble(7);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.value.");
				}
				// COMMON VALUES: platformType, stationId
				try {
					rightPlatformType = input.getString(8);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.platformType.");
				}
				try {
					rightStationId = input.getInteger(9);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.stationId.");
				}

				resultLeft = "left sensorId[" + leftSensorId + "] sensorType[" + leftSensorType + "] timestamp["
						+ leftTimestamp + "] value[" + leftValue + "]";
				resultRight = "right sensorId[" + rightSensorId + "] sensorType[" + rightSensorType + "] timestamp["
						+ rightTimestamp + "] value[" + rightValue + "]";
				resultProjection = resultLeft + " - " + resultRight + " - platformType[" + rightPlatformType
						+ "] stationId[" + rightStationId + "]";
				collector.emit(new Values(leftSensorId, leftSensorType, leftTimestamp, leftValue, rightSensorId,
						rightSensorType, rightTimestamp, rightValue, rightPlatformType, rightStationId));
				break;
			case 2:
				// TICKETS: sensorType, platformId, timestamp, sum
				try {
					leftSensorType = input.getString(0);
				} catch (ClassCastException re) {
					System.err.println("Error converting left.sensorType.");
				}
				try {
					leftPlatformId = input.getInteger(1);
				} catch (ClassCastException re) {
					System.err.println("Error converting left.platformId.");
				}
				try {
					leftTimestamp = input.getLong(2);
				} catch (ClassCastException re) {
					System.err.println("Error converting left.timestamp.");
				}
				try {
					leftValue = input.getDouble(3);
				} catch (ClassCastException re) {
					System.err.println("Error converting left.value.");
				}
				// TRAINS: sensorType, platformId, timestamp, sum
				try {
					rightSensorType = input.getString(4);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.sensorType.");
				}
				try {
					rightPlatformId = input.getInteger(5);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.platformId.");
				}
				try {
					rightTimestamp = input.getLong(6);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.timestamp.");
				}
				try {
					rightValue = input.getDouble(7);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.value.");
				}

				resultLeft = "left sensorType[" + leftSensorType + "] timestamp[" + leftTimestamp + "] sum[" + leftValue
						+ "]";
				resultRight = "right sensorType[" + rightSensorType + "] timestamp[" + rightTimestamp + "] sum["
						+ rightValue + "]";
				resultProjection = resultLeft + " - " + resultRight + " - platformId[" + leftPlatformId + "]";
				collector.emit(new Values(leftSensorType, leftPlatformId, leftTimestamp, leftValue, rightSensorType,
						rightPlatformId, rightTimestamp, rightValue));
				break;
			default:
				// TICKETS: sensorId, sensorType, platformId, platformType, stationId,
				// timestamp, value
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
					leftTimestamp = input.getLong(5);
				} catch (ClassCastException re) {
					System.err.println("Error converting left.timestamp.");
				}
				try {
					leftValue = input.getDouble(6);
				} catch (ClassCastException re) {
					System.err.println("Error converting left.value.");
				}
				// TRAINS: sensorId, sensorType, platformId, platformType, stationId, timestamp,
				// value
				try {
					rightSensorId = input.getInteger(7);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.sensorId.");
				}
				try {
					rightSensorType = input.getString(8);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.sensorType.");
				}
				try {
					rightPlatformId = input.getInteger(9);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.platformId.");
				}
				try {
					rightPlatformType = input.getString(10);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.platformType.");
				}
				try {
					rightStationId = input.getInteger(11);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.stationId.");
				}
				try {
					rightTimestamp = input.getLong(12);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.timestamp.");
				}
				try {
					rightValue = input.getDouble(13);
				} catch (ClassCastException re) {
					System.err.println("Error converting right.value.");
				}
				resultLeft = "left sensorId[" + leftSensorId + "] sensorType[" + leftSensorType + "] platformId["
						+ leftPlatformId + "] platformType[" + leftPlatformType + "] stationId[" + leftStationId
						+ "] timestamp[" + leftTimestamp + "] value[" + leftValue + "]";

				resultRight = "right sensorId[" + rightSensorId + "] sensorType[" + rightSensorType + "] platformId["
						+ rightPlatformId + "] platformType[" + rightPlatformType + "] stationId[" + rightStationId
						+ "] timestamp[" + rightTimestamp + "] value[" + rightValue + "]";
				resultProjection = resultLeft + " - " + resultRight;
				collector.emit(new Values(leftSensorId, leftSensorType, leftPlatformId, leftPlatformType, leftStationId,
						leftTimestamp, leftValue, rightSensorId, rightSensorType, rightPlatformId, rightPlatformType,
						rightStationId, rightTimestamp, rightValue));
				break;
			}

			result.add(input.toString());

			// collector.ack(input);
			// print here or wait until the application finishes to execute the cleanup()
			// method
			// logger.info(resultProjection);
		} finally {
			timeContext.stop();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		switch (projectionId) {
		case 1:
			// TICKETS: sensorId, sensorType, timestamp, value
			// TRAINS: sensorId, sensorType, timestamp, value
			// COMMON VALUES: platformType, stationId
			declarer.declare(
					new Fields(MqttSensors.FIELD_SENSOR_ID.getValue(), MqttSensors.FIELD_SENSOR_TYPE.getValue(),
							MqttSensors.FIELD_TIMESTAMP.getValue(), MqttSensors.FIELD_VALUE.getValue(),
							MqttSensors.FIELD_SENSOR_ID.getValue(), MqttSensors.FIELD_SENSOR_TYPE.getValue(),
							MqttSensors.FIELD_TIMESTAMP.getValue(), MqttSensors.FIELD_VALUE.getValue(),
							MqttSensors.FIELD_PLATFORM_TYPE.getValue(), MqttSensors.FIELD_STATION_ID.getValue()));
			break;
		case 2:
			// TICKETS: sensorType, platformId, timestamp, sum
			// TRAINS: sensorType, platformId, timestamp, sum
			declarer.declare(
					new Fields(MqttSensors.FIELD_SENSOR_TYPE.getValue(), MqttSensors.FIELD_PLATFORM_ID.getValue(),
							MqttSensors.FIELD_TIMESTAMP.getValue(), MqttSensors.FIELD_SUM.getValue(),
							MqttSensors.FIELD_SENSOR_TYPE.getValue(), MqttSensors.FIELD_PLATFORM_ID.getValue(),
							MqttSensors.FIELD_TIMESTAMP.getValue(), MqttSensors.FIELD_SUM.getValue()));
			break;
		default:
			// TICKETS:sensorId,sensorType,platformId,platformType,stationId,timestamp,value
			// TRAINS:sensorId,sensorType,platformId,platformType,stationId,timestamp,value
			declarer.declare(
					new Fields(MqttSensors.FIELD_SENSOR_ID.getValue(), MqttSensors.FIELD_SENSOR_TYPE.getValue(),
							MqttSensors.FIELD_PLATFORM_ID.getValue(), MqttSensors.FIELD_PLATFORM_TYPE.getValue(),
							MqttSensors.FIELD_STATION_ID.getValue(), MqttSensors.FIELD_TIMESTAMP.getValue(),
							MqttSensors.FIELD_VALUE.getValue(), MqttSensors.FIELD_SENSOR_ID.getValue(),
							MqttSensors.FIELD_SENSOR_TYPE.getValue(), MqttSensors.FIELD_PLATFORM_ID.getValue(),
							MqttSensors.FIELD_PLATFORM_TYPE.getValue(), MqttSensors.FIELD_STATION_ID.getValue(),
							MqttSensors.FIELD_TIMESTAMP.getValue(), MqttSensors.FIELD_VALUE.getValue()));
			break;
		}
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
			this.projection =
					// TICKETS: sensorId, sensorType, timestamp, value
					MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":" + MqttSensors.FIELD_SENSOR_ID.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":"
							+ MqttSensors.FIELD_SENSOR_TYPE.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":"
							+ MqttSensors.FIELD_TIMESTAMP.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":" + MqttSensors.FIELD_VALUE.getValue()
							+ ","
							// TRAINS: sensorId, sensorType, timestamp, value
							+ MqttSensors.SPOUT_STATION_01_TRAINS.getValue() + ":"
							+ MqttSensors.FIELD_SENSOR_ID.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TRAINS.getValue() + ":"
							+ MqttSensors.FIELD_SENSOR_TYPE.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TRAINS.getValue() + ":"
							+ MqttSensors.FIELD_TIMESTAMP.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TRAINS.getValue() + ":" + MqttSensors.FIELD_VALUE.getValue()
							// COMMON VALUES: platformType, stationId
							+ "," + MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":"
							+ MqttSensors.FIELD_PLATFORM_TYPE.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":"
							+ MqttSensors.FIELD_STATION_ID.getValue();
			break;
		case 2:
			this.projection =
					// TICKETS: sensorType, platformId, timestamp, sum
					MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue() + ":" + MqttSensors.FIELD_SENSOR_TYPE.getValue() + ","
							+ MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue() + ":"
							+ MqttSensors.FIELD_PLATFORM_ID.getValue() + ","
							+ MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue() + ":"
							+ MqttSensors.FIELD_TIMESTAMP.getValue() + ","
							+ MqttSensors.BOLT_SENSOR_TICKET_SUM.getValue() + ":" + MqttSensors.FIELD_SUM.getValue()
							+ ","
							// TRAINS: sensorType, platformId, timestamp, sum
							+ MqttSensors.BOLT_SENSOR_TRAIN_SUM.getValue() + ":"
							+ MqttSensors.FIELD_SENSOR_TYPE.getValue() + ","
							+ MqttSensors.BOLT_SENSOR_TRAIN_SUM.getValue() + ":"
							+ MqttSensors.FIELD_PLATFORM_ID.getValue() + ","
							+ MqttSensors.BOLT_SENSOR_TRAIN_SUM.getValue() + ":"
							+ MqttSensors.FIELD_TIMESTAMP.getValue() + ","
							+ MqttSensors.BOLT_SENSOR_TRAIN_SUM.getValue() + ":" + MqttSensors.FIELD_SUM.getValue();
			break;
		default:
			this.projection =
					// TICKETS: sensorId, sensorType, platformId, platformType, stationId,
					// timestamp, value
					MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":" + MqttSensors.FIELD_SENSOR_ID.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":"
							+ MqttSensors.FIELD_SENSOR_TYPE.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":"
							+ MqttSensors.FIELD_PLATFORM_ID.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":"
							+ MqttSensors.FIELD_PLATFORM_TYPE.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":"
							+ MqttSensors.FIELD_STATION_ID.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":"
							+ MqttSensors.FIELD_TIMESTAMP.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TICKETS.getValue() + ":" + MqttSensors.FIELD_VALUE.getValue()
							+ ","
							// TRAINS: sensorId, sensorType, platformId, platformType, stationId, timestamp,
							// value
							+ MqttSensors.SPOUT_STATION_01_TRAINS.getValue() + ":"
							+ MqttSensors.FIELD_SENSOR_ID.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TRAINS.getValue() + ":"
							+ MqttSensors.FIELD_SENSOR_TYPE.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TRAINS.getValue() + ":"
							+ MqttSensors.FIELD_PLATFORM_ID.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TRAINS.getValue() + ":"
							+ MqttSensors.FIELD_PLATFORM_TYPE.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TRAINS.getValue() + ":"
							+ MqttSensors.FIELD_STATION_ID.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TRAINS.getValue() + ":"
							+ MqttSensors.FIELD_TIMESTAMP.getValue() + ","
							+ MqttSensors.SPOUT_STATION_01_TRAINS.getValue() + ":" + MqttSensors.FIELD_VALUE.getValue();
			break;
		}
	}
}
