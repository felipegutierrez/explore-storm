package org.sense.storm.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SensorMapperBolt extends BaseRichBolt {

	private static final long serialVersionUID = -6408993991316811358L;

	// Create instance for OutputCollector which collects and emits tuples to
	// produce output
	private OutputCollector collector;

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
		Double value = 0.0;
		try {
			sensorId = Integer.parseInt(arr[0]);
		} catch (NumberFormatException re) {
			System.err.println("Error converting arr0.");
		}
		try {
			sensorType = String.valueOf(arr[1]);
		} catch (ClassCastException re) {
			System.err.println("Error converting arr1.");
		}
		try {
			platformId = Integer.parseInt(arr[2]);
		} catch (NumberFormatException re) {
			System.err.println("Error converting arr2.");
		}
		try {
			platformType = String.valueOf(arr[3]);
		} catch (ClassCastException re) {
			System.err.println("Error converting arr3.");
		}
		try {
			stationId = Integer.parseInt(arr[4]);
		} catch (NumberFormatException re) {
			System.err.println("Error converting arr4.");
		}
		try {
			value = Double.parseDouble(arr[5]);
		} catch (NumberFormatException re) {
			System.err.println("Error converting arr5.");
		}

		collector.emit(new Values(sensorId, sensorType, platformId, platformType, stationId, value));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sensor-id", "sensor-type", "platform-id", "platform-type", "station-id", "value"));
	}

	@Override
	public void cleanup() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
