package org.sense.storm;

import java.util.Scanner;

import org.apache.log4j.Logger;
import org.sense.storm.topology.MqttSensorJoinTopology;
import org.sense.storm.topology.MqttSensorSumTopology;
import org.sense.storm.topology.MqttSensorTopology;

public class App {

	final static Logger logger = Logger.getLogger(App.class);

	public static void main(String[] args) throws Exception {
		try {
			int app = 0;
			do {
				// @formatter:off
				System.out.println("0  - exit");
				System.out.println("1  - reading from train station sensors ");
				System.out.println("2  - reading multiple data stream sensors from train station topics and performing a JOIN operation");
				System.out.println("3  - reading multiple data stream sensors from train station topics and performing an AGGREGATE operation");
				// @formatter:on

				String appId = "0";
				String env = "local";
				String ipAddress = "127.0.0.1";
				if (args != null && args.length > 0) {
					appId = args[0];
					if (appId.matches("-?\\d+")) {
						System.out.println("    Application choosed: " + appId);
					} else {
						appId = "999";
					}
					if (args.length > 1) {
						env = args[1];
						if (!("cluster".equalsIgnoreCase(env) || "local".equalsIgnoreCase(env))) {
							System.err.println("Wrong environment selected.");
							System.out.print(
									"Please enter [cluster] or [local] to specify where you want to run your application: ");
							env = (new Scanner(System.in)).nextLine();
						}
						if (args.length > 2) {
							ipAddress = args[2];
							if (!validIP(ipAddress)) {
								ipAddress = "127.0.0.1";
								System.err.println("IP address invalid. Using the default IP address: " + ipAddress);
							} else {
								System.out.println("Valid IP address: " + ipAddress);
							}
						}
					}
				} else {
					System.out.print("    Please enter which application you want to run: ");
					appId = (new Scanner(System.in)).nextLine();
				}

				app = Integer.valueOf(appId);
				switch (app) {
				case 0:
					System.out.println("bis später");
					break;
				case 1:
					new MqttSensorTopology(env);
					app = 0;
					break;
				case 2:
					new MqttSensorJoinTopology(env, ipAddress);
					app = 0;
					break;
				case 3:
					new MqttSensorSumTopology(env, ipAddress);
					app = 0;
					break;
				default:
					args = null;
					System.out.println("No application selected [" + app + "] ");
					break;
				}
			} while (app != 0);
		} catch (Exception ce) {
			logger.error(ce.getMessage(), ce.getCause());
		}
	}

	public static boolean validIP(String ip) {
		try {
			if (ip == null || ip.isEmpty()) {
				return false;
			}

			String[] parts = ip.split("\\.");
			if (parts.length != 4) {
				return false;
			}

			for (String s : parts) {
				int i = Integer.parseInt(s);
				if ((i < 0) || (i > 255)) {
					return false;
				}
			}
			if (ip.endsWith(".")) {
				return false;
			}

			return true;
		} catch (NumberFormatException nfe) {
			return false;
		}
	}
}
