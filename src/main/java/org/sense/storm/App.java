package org.sense.storm;

import java.util.Scanner;

import org.apache.log4j.Logger;
import org.sense.storm.topology.MqttSensorJoinTopology;
import org.sense.storm.topology.MqttSensorTopology;

public class App {

	final static Logger logger = Logger.getLogger(App.class);

	public static void main(String[] args) throws Exception {
		try {
			int app = 0;
			do {
				logger.debug("debuuuuuug");
				if (logger.isDebugEnabled()) {
					System.out.println("debug");
				}
				// @formatter:off
				logger.info("0  - exit");
				logger.info("1  - reading from train station sensors ");
				logger.info("2  - reading multiple data stream sensors from train station topics");
				// @formatter:on

				String msg = "0";
				if (args != null && args.length > 0) {
					msg = args[0];
					if (msg.matches("-?\\d+")) {
						logger.info("    Application choosed: " + msg);
					} else {
						msg = "999";
					}
				} else {
					logger.info("    Please enter which application you want to run: ");
					msg = (new Scanner(System.in)).nextLine();
				}

				app = Integer.valueOf(msg);
				switch (app) {
				case 0:
					logger.info("bis später");
					break;
				case 1:
					// @formatter:off
					logger.info("Application 1 selected");
					logger.info("Please enter [cluster] or [local] to specify where you want to run your application: ");
					// @formatter:on
					msg = (new Scanner(System.in)).nextLine();
					new MqttSensorTopology(msg);
					app = 0;
					break;
				case 2:
					// @formatter:off
					logger.info("Application 2 selected");
					logger.info("Please enter [cluster] or [local] to specify where you want to run your application: ");
					// @formatter:on
					msg = (new Scanner(System.in)).nextLine();
					new MqttSensorJoinTopology(msg);
					app = 0;
					break;
				default:
					args = null;
					logger.info("No application selected [" + app + "] ");
					break;
				}
			} while (app != 0);
		} catch (Exception ce) {
			System.err.println(ce.getMessage());
			ce.printStackTrace();
		}
	}
}
