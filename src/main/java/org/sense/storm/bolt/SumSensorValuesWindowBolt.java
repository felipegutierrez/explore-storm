package org.sense.storm.bolt;

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
import org.sense.storm.utils.SensorType;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

/**
 * This is a Bolt implementation that sum all values from sensors of the same
 * type.
 */
public class SumSensorValuesWindowBolt extends BaseWindowedBolt {

	private static final long serialVersionUID = 6005737461658868444L;
	private static final Logger logger = Logger.getLogger(SumSensorValuesWindowBolt.class);

	private OutputCollector collector;
	private SensorType sensorType;
	private Meter tupleMeter;
	private Timer tupleTimer;
	private Histogram tupleHistogram;

	public SumSensorValuesWindowBolt(SensorType sensorType) {
		this.sensorType = sensorType;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.tupleMeter = context.registerMeter("meterSum-" + this.sensorType.getValue());
		this.tupleTimer = context.registerTimer("timerSum-" + this.sensorType.getValue());
		this.tupleHistogram = context.registerHistogram("histogramSum-" + this.sensorType.getValue());
	}

	@Override
	public void execute(TupleWindow inputWindow) {

		final Timer.Context timeContext = this.tupleTimer.time();
		this.tupleMeter.mark();
		// Map<Integer, Double> sum = new HashMap<Integer, Double>();
		Map<Integer, Pair<Long, Double>> sum = new HashMap<Integer, Pair<Long, Double>>();

		try {
			if (this.sensorType == null) {
				logger.error("You must configure the SensorType before use this Bolt", new Exception());
			}
			for (Tuple tuple : inputWindow.get()) {

				Integer sensorId = null;
				String sensorType = null;
				Integer platformId = null;
				String platformType = null;
				Integer stationId = null;
				Long timestamp = null;
				Double value = null;

				try {
					sensorType = tuple.getString(1);
				} catch (ClassCastException re) {
					logger.error("Error converting sensorType.", re.getCause());
				}
				// Only compute the sum for a specific sensor type
				if (this.sensorType.getValue().equals(sensorType)) {
					try {
						platformId = tuple.getInteger(2);
					} catch (ClassCastException re) {
						logger.error("Error converting platformId.", re.getCause());
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

					if (sum.containsKey(platformId)) {
						Double total = sum.get(platformId).getSecond() + value;
						sum.put(platformId, new Pair<Long, Double>(timestamp, total));
					} else {
						sum.put(platformId, new Pair<Long, Double>(timestamp, value));
					}
				}
			}
			for (Map.Entry<Integer, Pair<Long, Double>> entry : sum.entrySet()) {
				// outputs: sensorType, platformId, timestamp, sum
				collector.emit(new Values(this.sensorType.getValue(), entry.getKey(), entry.getValue().getFirst(),
						entry.getValue().getSecond()));
			}
		} finally {
			timeContext.stop();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(MqttSensors.FIELD_SENSOR_TYPE.getValue(), MqttSensors.FIELD_PLATFORM_ID.getValue(),
				MqttSensors.FIELD_TIMESTAMP.getValue(), MqttSensors.FIELD_SUM.getValue()));
	}
}
