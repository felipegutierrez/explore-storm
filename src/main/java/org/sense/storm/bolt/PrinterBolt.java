package org.sense.storm.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class PrinterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 3194181787824788475L;

	final static Logger logger = Logger.getLogger(PrinterBolt.class);

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		logger.info(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}
}
