package org.sense.storm.bolt;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.sense.storm.utils.MqttSensors;
import org.sense.storm.utils.Pair;
import org.sense.storm.utils.Sensor;
import org.sense.storm.utils.SensorType;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

/**
 * This is a Bolt implementation that sum all values from sensors of the same
 * type.
 */
public class SensorAggregateValuesWindowBolt extends BaseWindowedBolt {

	private static final long serialVersionUID = -7368868818571376139L;
	private static final Logger logger = Logger.getLogger(SensorAggregateValuesWindowBolt.class);

	private OutputCollector collector;
	private Meter tupleMeter;
	private Timer tupleTimer;
	private Histogram tupleHistogram;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.tupleMeter = context.registerMeter("meterAgg");
		this.tupleTimer = context.registerTimer("timerAgg");
		this.tupleHistogram = context.registerHistogram("histogramAgg");
	}

	@Override
	public void execute(TupleWindow inputWindow) {

		final Timer.Context timeContext = this.tupleTimer.time();
		this.tupleMeter.mark();
		Map<String, Pair<Sensor, Long>> aggregatedValues = new HashMap<String, Pair<Sensor, Long>>();

		try {
			for (Tuple tuple : inputWindow.get()) {
				Integer sensorId = 0;
				String sensorType = "";
				Integer platformId = 0;
				String platformType = "";
				Integer stationId = 0;
				Long timestamp = 0L;
				Double value = 0.0;

				try {
					sensorId = tuple.getInteger(0);
				} catch (ClassCastException re) {
					logger.error("Error converting sensorId.", re.getCause());
				}
				try {
					sensorType = tuple.getString(1);
				} catch (ClassCastException re) {
					logger.error("Error converting sensorType.", re.getCause());
				}
				try {
					platformId = tuple.getInteger(2);
				} catch (ClassCastException re) {
					logger.error("Error converting platformId.", re.getCause());
				}
				try {
					platformType = tuple.getString(3);
				} catch (ClassCastException re) {
					logger.error("Error converting platformType.", re.getCause());
				}
				try {
					stationId = tuple.getInteger(4);
				} catch (ClassCastException re) {
					logger.error("Error converting stationId.", re.getCause());
				}
				try {
					timestamp = tuple.getLong(5);
				} catch (ClassCastException re) {
					logger.error("Error converting timestamp.", re.getCause());
				}
				try {
					value = tuple.getDouble(6);
				} catch (ClassCastException re) {
					logger.error("Error converting value.", re.getCause());
				}

				// generate a key based on Station-platform-sensorType in order to aggregate
				// values of sensor with same type, on the same platform and same train station
				String compositeKey = "station-" + stationId + "-platform-" + platformId + "-sensor-" + sensorType;
				Sensor sensor = null;

				if (aggregatedValues.containsKey(compositeKey)) {
					Pair<Sensor, Long> currentSensor = aggregatedValues.get(compositeKey);
					Double totalValue = currentSensor.getFirst().getValue() + value;
					Long totalCount = currentSensor.getSecond() + 1;

					sensor = new Sensor(sensorId, sensorType, platformId, platformType, stationId, timestamp,
							totalValue);
					aggregatedValues.put(compositeKey, new Pair<Sensor, Long>(sensor, totalCount));
				} else {
					sensor = new Sensor(sensorId, sensorType, platformId, platformType, stationId, timestamp, value);
					aggregatedValues.put(compositeKey, new Pair<Sensor, Long>(sensor, 1L));
				}
				System.out.println("Processing key[" + compositeKey + "] value[" + sensor + "]");
			}
			for (Map.Entry<String, Pair<Sensor, Long>> entry : aggregatedValues.entrySet()) {

				String compositeKey = entry.getKey();
				Sensor currentSensor = entry.getValue().getFirst();
				Long totalCount = entry.getValue().getSecond();

				if (currentSensor.getSensorType().equals(SensorType.COUNTER_PEOPLE.getValue())
						|| currentSensor.getSensorType().equals(SensorType.COUNTER_TICKETS.getValue())
						|| currentSensor.getSensorType().equals(SensorType.COUNTER_TRAINS.getValue())) {

					// outputs the SUM for COUNTER sensors
					collector.emit(new Values(compositeKey, currentSensor, totalCount));
				} else if (currentSensor.getSensorType().equals(SensorType.TEMPERATURE.getValue())
						|| currentSensor.getSensorType().equals(SensorType.LIFT_VIBRATION.getValue())) {

					// outputs the AVERAGE for TEMPERATURE sensors
					Double average = currentSensor.getValue() / totalCount;
					Sensor sensor = new Sensor(null, currentSensor.getSensorType(), currentSensor.getPlatformId(),
							currentSensor.getPlatformType(), currentSensor.getStationId(),
							Calendar.getInstance().getTimeInMillis(), average);
					collector.emit(new Values(compositeKey, sensor, totalCount));
				} else {
					logger.error("Sensor not registered to compute aggregation.");
				}
			}
		} finally {
			timeContext.stop();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(MqttSensors.FIELD_SENSOR_TYPE.getValue(), MqttSensors.FIELD_SENSOR.getValue(),
				MqttSensors.FIELD_COUNT.getValue()));
	}
}
