package org.sense.storm.bolt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class SensorPrintBolt extends BaseRichBolt {

	private static final long serialVersionUID = 7146201246991885765L;

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
		Integer sensorId = input.getInteger(0);
		String sensorType = input.getString(1);
		Integer platformId = input.getInteger(2);
		String platformType = input.getString(3);
		Integer stationId = input.getInteger(4);
		Double value = input.getDouble(5);

		result.add(input.toString());

		collector.ack(input);
		// print here or wait until the application finishes to execute the cleanup()
		// method
		// System.out.println(input.toString());
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
		System.out.println("-- Word Counter [" + name + "-" + id + "] --");
		for (Iterator<String> iterator = result.iterator(); iterator.hasNext();) {
			String string = (String) iterator.next();
			System.out.println(string);
		}
	}
}
