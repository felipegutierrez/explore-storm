package org.sense.storm.utils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Sensor implements Serializable {

	private static final long serialVersionUID = -5576739994158543478L;

	private Integer sensorId;
	private String sensorType;
	private Integer platformId;
	private String platformType;
	private Integer stationId;
	private Long timestamp;
	private Double value;

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public Sensor() {
	}

	public Sensor(Integer sensorId, String sensorType, Integer platformId, String platformType, Integer stationId,
			Long timestamp, Double value) {
		this.sensorId = sensorId;
		this.sensorType = sensorType;
		this.platformId = platformId;
		this.platformType = platformType;
		this.stationId = stationId;
		this.timestamp = timestamp;
		this.value = value;
	}

	public Integer getSensorId() {
		return sensorId;
	}

	public void setSensorId(Integer sensorId) {
		this.sensorId = sensorId;
	}

	public String getSensorType() {
		return sensorType;
	}

	public void setSensorType(String sensorType) {
		this.sensorType = sensorType;
	}

	public Integer getPlatformId() {
		return platformId;
	}

	public void setPlatformId(Integer platformId) {
		this.platformId = platformId;
	}

	public String getPlatformType() {
		return platformType;
	}

	public void setPlatformType(String platformType) {
		this.platformType = platformType;
	}

	public Integer getStationId() {
		return stationId;
	}

	public void setStationId(Integer stationId) {
		this.stationId = stationId;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Sensor [sensorId=" + sensorId + ", sensorType=" + sensorType + ", platformId=" + platformId
				+ ", platformType=" + platformType + ", stationId=" + stationId + ", timestamp="
				+ sdf.format(new Date(timestamp)) + ", value=" + value + "]";
	}
}
