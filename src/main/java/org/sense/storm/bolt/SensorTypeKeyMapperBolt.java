package org.sense.storm.bolt;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SensorTypeKeyMapperBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5454803133389789227L;
	private static final Logger logger = Logger.getLogger(SensorTypeKeyMapperBolt.class);

	// Create instance for OutputCollector which collects and emits tuples to
	// produce output
	private OutputCollector collector;
	private Fields outFields;

	public SensorTypeKeyMapperBolt(Fields outFields) {
		this.outFields = outFields;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String topic = input.getString(0);
		String payload = input.getString(1);
		String[] arr = payload.split("\\|");

		Integer sensorId = 0;
		String sensorType = "";
		Integer platformId = 0;
		String platformType = "";
		Integer stationId = 0;
		Long timestamp = 0L;
		Double value = 0.0;
		try {
			sensorId = Integer.parseInt(arr[0]);
		} catch (NumberFormatException re) {
			if (logger.isDebugEnabled()) {
				logger.debug("Error converting arr0.");
			}
		}
		try {
			sensorType = String.valueOf(arr[1]);
		} catch (ClassCastException re) {
			if (logger.isDebugEnabled()) {
				logger.debug("Error converting arr1.");
			}
		}
		try {
			platformId = Integer.parseInt(arr[2]);
		} catch (NumberFormatException re) {
			if (logger.isDebugEnabled()) {
				logger.debug("Error converting arr2.");
			}
		}
		try {
			platformType = String.valueOf(arr[3]);
		} catch (ClassCastException re) {
			if (logger.isDebugEnabled()) {
				logger.debug("Error converting arr3.");
			}
		}
		try {
			stationId = Integer.parseInt(arr[4]);
		} catch (NumberFormatException re) {
			if (logger.isDebugEnabled()) {
				logger.debug("Error converting arr4.");
			}
		}
		try {
			timestamp = Long.parseLong(arr[5]);
		} catch (NumberFormatException re) {
			if (logger.isDebugEnabled()) {
				logger.debug("Error converting arr5.");
			}
		}
		try {
			value = Double.parseDouble(arr[6]);
		} catch (NumberFormatException re) {
			if (logger.isDebugEnabled()) {
				logger.debug("Error converting arr6.");
			}
		}

		collector.emit(new Values(sensorId, sensorType, platformId, platformType, stationId, timestamp, value));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outFields);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
