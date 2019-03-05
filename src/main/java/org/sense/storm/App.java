package org.sense.storm;

import java.util.Scanner;

import org.sense.storm.topology.MqttSensorTopology;

public class App {
	public static void main(String[] args) throws Exception {
		try {
			int app = 0;
			do {
				// @formatter:off
				System.out.println("0  - exit");
				System.out.println("1  - reading from station sensors ");
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
					System.out.println("App 1 selected");
					new MqttSensorTopology();
					app = 0;
					break;
				default:
					args = null;
					System.out.println("No application selected [" + app + "] ");
					break;
				}
			} while (app != 0);
		} catch (Exception ce) {
			System.err.println(ce.getMessage());
			ce.printStackTrace();
		}
	}
}
