package org.sense.storm.spout;

import java.net.URISyntaxException;
import java.util.Map;

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

public class MqttSensorSpout extends BaseRichSpout {

	private static final long serialVersionUID = 5407831157851644153L;

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

	public MqttSensorSpout(String topic) {
		this(DEFAUL_HOST, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttSensorSpout(String host, String topic) {
		this(host, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttSensorSpout(String host, int port, String topic) {
		this(host, port, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttSensorSpout(String host, int port, String topic, QoS qos) {
		this.host = host;
		this.port = port;
		this.topic = topic;
		this.qos = qos;
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;

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
		// TODO Auto-generated method stub

	}

	public void deactivate() {
		// TODO Auto-generated method stub

	}

	public void nextTuple() {
		try {
			while (blockingConnection.isConnected()) {
				Message message = blockingConnection.receive();
				String payload = new String(message.getPayload());

				// System.out.println("message[" + topic + "]: " + payload);

				message.ack();

				collector.emit(new Values(topic, payload));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("topic", "payload"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
