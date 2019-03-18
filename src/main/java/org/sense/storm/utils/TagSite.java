package org.sense.storm.utils;

public enum TagSite {
	SITE("site"), CLUSTER("cluster"), EDGE("edge"), UNTAGGED("untagged");

	private String value;

	TagSite(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
