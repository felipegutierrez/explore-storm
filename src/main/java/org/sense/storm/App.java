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

				String msg = "0";
				if (args != null && args.length > 0) {
					msg = args[0];
					if (msg.matches("-?\\d+")) {
						System.out.println("    Application choosed: " + msg);
					} else {
						msg = "999";
					}
				} else {
					System.out.print("    Please enter which application you want to run: ");
					msg = (new Scanner(System.in)).nextLine();
				}

				app = Integer.valueOf(msg);
				switch (app) {
				case 0:
					System.out.println("bis später");
					break;
				case 1:
					// @formatter:off
					System.out.println("Application 1 selected");
					System.out.print("Please enter [cluster] or [local] to specify where you want to run your application: ");
					// @formatter:on
					msg = (new Scanner(System.in)).nextLine();
					new MqttSensorTopology(msg);
					app = 0;
					break;
				case 2:
					// @formatter:off
					System.out.println("Application 2 selected");
					System.out.print("Please enter [cluster] or [local] to specify where you want to run your application: ");
					// @formatter:on
					msg = (new Scanner(System.in)).nextLine();
					new MqttSensorJoinTopology(msg);
					app = 0;
					break;
				case 3:
					// @formatter:off
					System.out.println("Application 3 selected");
					System.out.print("Please enter [cluster] or [local] to specify where you want to run your application: ");
					// @formatter:on
					msg = (new Scanner(System.in)).nextLine();
					new MqttSensorSumTopology(msg);
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
}
