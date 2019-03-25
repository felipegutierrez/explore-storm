package org.sense.storm.spout;

import java.net.URISyntaxException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

public class MqttSensorDetailSpout extends BaseRichSpout {

	final static Logger logger = Logger.getLogger(MqttSensorDetailSpout.class);
	private static final long serialVersionUID = 27262073917747566L;

	// Create instance for TopologyContext which contains topology data.
	private TopologyContext context;
	// Create instance for SpoutOutputCollector which passes tuples to bolt.
	private SpoutOutputCollector collector;
	private boolean completed = false;

	final private static String DEFAUL_HOST = "127.0.0.1";
	final private static int DEFAUL_PORT = 1883;

	private String host;
	private int port;
	private String topic;
	private QoS qos;
	private BlockingConnection blockingConnection;

	private Fields outFields;

	private Meter tupleMeter;
	private Timer tupleTimer;

	public MqttSensorDetailSpout(String topic, Fields outFields) {
		this(DEFAUL_HOST, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE, outFields);
	}

	public MqttSensorDetailSpout(String host, String topic, Fields outFields) {
		this(host, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE, outFields);
	}

	public MqttSensorDetailSpout(String host, int port, String topic, Fields outFields) {
		this(host, port, topic, QoS.AT_LEAST_ONCE, outFields);
	}

	public MqttSensorDetailSpout(String host, int port, String topic, QoS qos, Fields outFields) {
		this.host = host;
		this.port = port;
		this.topic = topic;
		this.qos = qos;
		this.outFields = outFields;
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;
		this.tupleMeter = context.registerMeter("meterSpout-" + this.topic);
		this.tupleTimer = context.registerTimer("timerSpout-" + this.topic);
		MQTT mqtt = new MQTT();
		try {
			mqtt.setHost(host, port);

			blockingConnection = mqtt.blockingConnection();
			blockingConnection.connect();

			byte[] qoses = blockingConnection.subscribe(new Topic[] { new Topic(topic, qos) });
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			if (blockingConnection != null) {
				blockingConnection.disconnect();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void activate() {
	}

	public void deactivate() {
	}

	public void nextTuple() {
		final Timer.Context timeContext = this.tupleTimer.time();
		this.tupleMeter.mark();
		try {
			while (blockingConnection.isConnected()) {
				Message message = blockingConnection.receive();
				String payload = new String(message.getPayload());
				logger.debug("message[" + topic + "]: " + payload);

				message.ack();

				String[] arr = payload.split("\\|");
				Integer sensorId = 0;
				String sensorType = "";
				Integer platformId = 0;
				String platformType = "";
				Integer stationId = 0;
				Double value = 0.0;
				try {
					sensorId = Integer.parseInt(arr[0]);
				} catch (ClassCastException re) {
					logger.error("Error converting sensorId.", re.getCause());
				}
				try {
					sensorType = String.valueOf(arr[1]);
				} catch (ClassCastException re) {
					logger.error("Error converting sensorType.", re.getCause());
				}
				try {
					platformId = Integer.parseInt(arr[2]);
				} catch (ClassCastException re) {
					logger.error("Error converting platformId.", re.getCause());
				}
				try {
					platformType = String.valueOf(arr[3]);
				} catch (ClassCastException re) {
					logger.error("Error converting platformType.", re.getCause());
				}
				try {
					stationId = Integer.parseInt(arr[4]);
				} catch (ClassCastException re) {
					logger.error("Error converting stationId.", re.getCause());
				}
				try {
					value = Double.parseDouble(arr[5]);
				} catch (ClassCastException re) {
					logger.error("Error converting value.", re.getCause());
				}
				collector.emit(new Values(sensorId, sensorType, platformId, platformType, stationId, value));
			}
		} catch (Exception e) {
			logger.error("Error: ", e.getCause());
			e.printStackTrace();
		} finally {
			timeContext.stop();
		}
	}

	public void ack(Object msgId) {
	}

	public void fail(Object msgId) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(outFields);
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
