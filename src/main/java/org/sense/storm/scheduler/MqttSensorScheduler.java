package org.sense.storm.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;

public class MqttSensorScheduler implements IScheduler {

	public static final String SCHEDULER_CLUSTER = "scheduler-cluster";
	public static final String SCHEDULER_EDGE = "scheduler-edge";

	public void prepare(Map conf) {

	}

	public void schedule(Topologies topologies, Cluster cluster) {
		Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
		Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();
		Map<Integer, SupervisorDetails> supervisors = new HashMap<Integer, SupervisorDetails>();

		for (SupervisorDetails s : supervisorDetails) {
			Map<String, Object> metadata = (Map<String, Object>) s.getSchedulerMeta();
			if (metadata.get(SCHEDULER_CLUSTER) != null) {
				supervisors.put((Integer) metadata.get(SCHEDULER_CLUSTER), s);
			} else if (metadata.get(SCHEDULER_EDGE) != null) {
				supervisors.put((Integer) metadata.get(SCHEDULER_EDGE), s);
			}
		}

		for (TopologyDetails t : topologyDetails) {
			if (cluster.needsScheduling(t))
				continue;
			StormTopology topology = t.getTopology();
			Map<String, Bolt> bolts = topology.get_bolts();
			Map<String, SpoutSpec> spouts = topology.get_spouts();

			JSONParser parser = new JSONParser();
			try {
				for (String name : bolts.keySet()) {
					Bolt bolt = bolts.get(name);
					JSONObject conf = (JSONObject) parser.parse(bolt.get_common().get_json_conf());
					if (conf.get(SCHEDULER_CLUSTER) != null && supervisors.get(conf.get(SCHEDULER_CLUSTER)) != null) {
						Integer gid = (Integer) conf.get(SCHEDULER_CLUSTER);
					} else if (conf.get(SCHEDULER_EDGE) != null && supervisors.get(conf.get(SCHEDULER_EDGE)) != null) {
						Integer gid = (Integer) conf.get(SCHEDULER_EDGE);
					}
				}
				for (String name : spouts.keySet()) {
					SpoutSpec spout = spouts.get(name);
					JSONObject conf = (JSONObject) parser.parse(spout.get_common().get_json_conf());
					if (conf.get(SCHEDULER_CLUSTER) != null && supervisors.get(conf.get(SCHEDULER_CLUSTER)) != null) {
						Integer gid = (Integer) conf.get(SCHEDULER_CLUSTER);
					} else if (conf.get(SCHEDULER_EDGE) != null && supervisors.get(conf.get(SCHEDULER_EDGE)) != null) {
						Integer gid = (Integer) conf.get(SCHEDULER_EDGE);
					}
				}
			} catch (ParseException pe) {
				pe.printStackTrace();
			}
		}
	}
}
