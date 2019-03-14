package org.sense.storm.utils;

public enum MqttSensors {

	TOPIC_STATION_01_PEOPLE("topic-station-01-people"), TOPIC_STATION_01_TRAINS("topic-station-01-trains"),
	TOPIC_STATION_01_TICKETS("topic-station-01-tickets"), TOPIC_STATION_02_PEOPLE("topic-station-02-people"),
	TOPIC_STATION_02_TRAINS("topic-station-02-trains"), TOPIC_STATION_02_TICKETS("topic-station-02-tickets"),

	FIELD_SENSOR_ID("sensorId"), FIELD_SENSOR_TYPE("sensorType"), FIELD_PLATFORM_ID("platformId"),
	FIELD_PLATFORM_TYPE("platformType"), FIELD_STATION_ID("stationId"), FIELD_VALUE("value"),

	SPOUT_STATION_01_PEOPLE("spout-station-01-people"), SPOUT_STATION_01_TRAINS("spout-station-01-trains"),
	SPOUT_STATION_01_TICKETS("spout-station-01-tickets"), SPOUT_STATION_02_PEOPLE("spout-station-02-people"),
	SPOUT_STATION_02_TRAINS("spout-station-02-trains"), SPOUT_STATION_02_TICKETS("spout-station-02-tickets"),

	BOLT_SENSOR_TICKET_SUM("bolt-sensor-ticket-sum"), BOLT_SENSOR_TRAIN_SUM("bolt-sensor-train-sum"),

	FIELD_SUM("sum"), FIELD_AVERAGE("average");

	private String value;

	MqttSensors(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
