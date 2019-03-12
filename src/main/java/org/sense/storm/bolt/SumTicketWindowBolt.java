package org.sense.storm.bolt;

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

public class SumTicketWindowBolt extends BaseWindowedBolt {

	private static final long serialVersionUID = 6005737461658868444L;

	final static Logger logger = Logger.getLogger(SumTicketWindowBolt.class);

	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(TupleWindow inputWindow) {
		Double computedValue = new Double(0);
		for (Tuple tuple : inputWindow.get()) {

			Double value = null;
			try {
				value = tuple.getDouble(5);
			} catch (ClassCastException re) {
				System.err.println("Error converting value.");
			}
			if (value != null) {
				computedValue = computedValue + value;
			}
		}
		collector.emit(new Values("ticket-counter", computedValue));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ticket-counter", "sum"));
	}
}
