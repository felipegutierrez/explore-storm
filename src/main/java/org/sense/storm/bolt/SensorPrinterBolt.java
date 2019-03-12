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

public class SensorPrinterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 7146201246991885765L;

	final static Logger logger = Logger.getLogger(SensorPrinterBolt.class);

	private Integer id;
	private String name;
	private OutputCollector collector;
	private List<String> result;

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
		Double value = null;

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
			value = input.getDouble(5);
		} catch (NumberFormatException re) {
			System.err.println("Error converting value.");
		}

		result.add(input.toString());

		collector.ack(input);
		// print here or wait until the application finishes to execute the cleanup()
		// method
		String result = "sensorId[" + sensorId + "] sensorType[" + sensorType + "] platformId[" + platformId
				+ "] platformType[" + platformType + "] stationId[" + stationId + "] value[" + value + "]";
		logger.info(result);
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
