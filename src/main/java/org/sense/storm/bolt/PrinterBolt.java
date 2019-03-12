package org.sense.storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class PrinterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 3194181787824788475L;

	final static Logger logger = Logger.getLogger(PrinterBolt.class);

	private OutputCollector collector;
	private List<String> result;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.result = new ArrayList<String>();
	}

	public void execute(Tuple input) {

		result.add(input.toString());

		collector.ack(input);
		// print here or wait until the application finishes to execute the cleanup()
		// method
		String result = "Printer Bolt: [" + input.toString() + "]";
		logger.info(result);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
