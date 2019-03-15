package org.sense.storm.metrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;

public class GraphiteMetricsConsumer implements IMetricsConsumer {

	final static Logger logger = Logger.getLogger(GraphiteMetricsConsumer.class);

	public static final String GRAPHITE_HOST = "metrics.graphite.host";
	public static final String GRAPHITE_PORT = "metrics.graphite.port";
	public static final String REPORTER_NAME = "metrics.reporter.name";
	public static final String GRAPHITE_OUTPUT_FORMAT = "metrics.graphite.format";
	public static final String GRAPHITE_OUTPUT_FORMAT_SINGLE = "metrics.graphite.format_single";

	private String graphiteHost = "127.0.0.1";
	private int graphitePort = 2003;
	private String reportName = "report";

	private String format = String.format("%s.%s.%s.%s.%s.%s.%s", OutputFormatConstructs.SRC_WORKER_HOST.magicSequence,
			OutputFormatConstructs.SRC_WORKER_PORT.magicSequence, OutputFormatConstructs.SRC_TOPOLOGY_NAME.magicSequence,
			OutputFormatConstructs.SRC_COMPONENT_ID.magicSequence, OutputFormatConstructs.SRC_TASK_ID.magicSequence,
			OutputFormatConstructs.METRIC_NAME.magicSequence, OutputFormatConstructs.METRIC_KEY.magicSequence);

	private String formatSingle = String.format("%s.%s.%s.%s.%s.%s",
			OutputFormatConstructs.SRC_WORKER_HOST.magicSequence, OutputFormatConstructs.SRC_WORKER_PORT.magicSequence,
			OutputFormatConstructs.SRC_TOPOLOGY_NAME.magicSequence,
			OutputFormatConstructs.SRC_COMPONENT_ID.magicSequence, OutputFormatConstructs.SRC_TASK_ID.magicSequence,
			OutputFormatConstructs.METRIC_NAME.magicSequence);

	@Override
	public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
			IErrorReporter errorReporter) {
		logger.info("preparing grapite metrics consumer");
		String graphiteHostFromConf = (String) stormConf.get(GRAPHITE_HOST);
		String graphitePortFromConf = (String) stormConf.get(GRAPHITE_PORT);
		String graphiteReportFromConf = (String) stormConf.get(REPORTER_NAME);
		String formatFromConf = (String) stormConf.get(GRAPHITE_OUTPUT_FORMAT);
		String formatSingleFromConf = (String) stormConf.get(GRAPHITE_OUTPUT_FORMAT_SINGLE);
		if (null != graphiteHostFromConf) {
			this.graphiteHost = graphiteHostFromConf;
		}
		if (null != graphitePortFromConf) {
			try {
				this.graphitePort = Integer.valueOf(graphitePortFromConf);
			} catch (NumberFormatException e) {
				logger.error(String.format("port must be an Integer, got: %s", graphitePortFromConf));
				throw e;
			}
		}
		if (null != graphiteReportFromConf) {
			this.reportName = graphiteReportFromConf;
		}
		if (null != formatFromConf) {
			this.format = formatFromConf;
		}
		if (null != formatSingleFromConf) {
			this.formatSingle = formatSingleFromConf;
		}
	}

	@Override
	public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
		try {
			logger.info(String.format("Connecting to graphite on %s:%d", graphiteHost, graphitePort));
			Socket socket = new Socket(graphiteHost, graphitePort);
			PrintWriter graphiteWriter = new PrintWriter(socket.getOutputStream(), true);
			logger.info(String.format("Graphite connected, got %d datapoints", dataPoints.size()));
			for (DataPoint p : dataPoints) {
				logger.info(String.format("Registering data point to graphite: %s, %s. Value type is: %s", p.name,
						p.value, p.value.getClass().getCanonicalName()));
				// yikes, we must use Run Time Type Information.
				if (p.value instanceof Map) {
					// storm uses raw map without generic types
					Set<Map.Entry> entries = ((Map) p.value).entrySet();
					for (Map.Entry e : entries) {
						graphiteWriter
								.printf("%s %s %d\n",
										OutputFormatConstructs.formatStringForGraphite(this.reportName, format,
												taskInfo, p.name, (String) e.getKey()),
										e.getValue(), taskInfo.timestamp);
					}
				} else if (p.value instanceof Number) {
					graphiteWriter.printf("%s %s %d\n", OutputFormatConstructs.formatStringForGraphite(this.reportName,
							formatSingle, taskInfo, p.name, null), p.value, taskInfo.timestamp);
				} else {
					// (relatively) Silent failure, as all kinds of metrics can be sent here
					logger.info(String.format("Got datapoint with unsupported type, %s",
							p.value.getClass().getCanonicalName()));
				}
			}
			graphiteWriter.close();
			socket.close();
		} catch (IOException e) {
			logger.error("IOException in graphite metrics consumer", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void cleanup() {
	}

	public enum OutputFormatConstructs {
		SRC_TOPOLOGY_NAME("%stn"), SRC_COMPONENT_ID("%sci"), SRC_TASK_ID("%sti"), SRC_WORKER_HOST("%swh"),
		SRC_WORKER_PORT("%swp"), UPDATE_INTERVAL_SEC("%uis"), METRIC_NAME("%mn"), METRIC_KEY("%mk");

		String magicSequence;

		OutputFormatConstructs(String magicSequence) {
			this.magicSequence = magicSequence;
		}

		/***
		 * Build the formatted string according to the required format This is not the
		 * most efficient implementation, but this occurs only on metric registration,
		 * which is not very frequent
		 */
		public static String formatStringForGraphite(String reportName, String formatString, TaskInfo taskInfo,
				String metricName, String metricKey) {
			String ret = formatString.replace(SRC_TOPOLOGY_NAME.magicSequence, reportName);
			ret = ret.replace(SRC_COMPONENT_ID.magicSequence, taskInfo.srcComponentId);
			ret = ret.replace(SRC_TASK_ID.magicSequence, String.valueOf(taskInfo.srcTaskId));
			ret = ret.replace(SRC_WORKER_HOST.magicSequence, taskInfo.srcWorkerHost);
			ret = ret.replace(SRC_WORKER_PORT.magicSequence, String.valueOf(taskInfo.srcWorkerPort));
			ret = ret.replace(UPDATE_INTERVAL_SEC.magicSequence, String.valueOf(taskInfo.updateIntervalSecs));
			ret = ret.replace(METRIC_NAME.magicSequence, metricName);
			if (null != metricKey) {
				ret = ret.replace(METRIC_KEY.magicSequence, metricKey);
			}
			return ret;
		}
	}
}
