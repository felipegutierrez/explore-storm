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

public class MqttSensorDetailSpout extends BaseRichSpout {

	private static final long serialVersionUID = 27262073917747566L;

	final static Logger logger = Logger.getLogger(MqttSensorDetailSpout.class);

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

	public static final String SPOUT_STATION_01_PEOPLE = "spout-station-01-people";
	public static final String SPOUT_STATION_01_TRAINS = "spout-station-01-trains";
	public static final String SPOUT_STATION_01_TICKETS = "spout-station-01-tickets";
	public static final String SPOUT_STATION_02_PEOPLE = "spout-station-02-people";
	public static final String SPOUT_STATION_02_TRAINS = "spout-station-02-trains";
	public static final String SPOUT_STATION_02_TICKETS = "spout-station-02-tickets";

	public static final String FIELD_SENSOR_ID = "sensorId";
	public static final String FIELD_SENSOR_TYPE = "sensorType";
	public static final String FIELD_PLATFORM_ID = "platformId";
	public static final String FIELD_PLATFORM_TYPE = "platformType";
	public static final String FIELD_STATION_ID = "stationId";
	public static final String FIELD_VALUE = "value";

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

				if (logger.isDebugEnabled()) {
					logger.info("message[" + topic + "]: " + payload);
				}

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
					System.err.println("Error converting arr3.");
				}
				try {
					value = Double.parseDouble(arr[5]);
				} catch (NumberFormatException re) {
					System.err.println("Error converting arr5.");
				}
				collector.emit(new Values(sensorId, sensorType, platformId, platformType, stationId, value));
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
		// declarer.declare(new Fields("topic", "sensorId", "sensorType", "platformId",
		// "platformType", "stationId", "value"));
		declarer.declare(outFields);
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
