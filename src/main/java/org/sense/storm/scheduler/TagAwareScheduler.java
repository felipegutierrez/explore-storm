package org.sense.storm.scheduler;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentCommon;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.shade.org.json.simple.parser.ParseException;

@SuppressWarnings("unused")
public class TagAwareScheduler implements IScheduler {

	final static Logger logger = Logger.getLogger(TagAwareScheduler.class);

	private final String untaggedTag = "untagged";

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf) {

	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		tagAwareSchedule(topologies, cluster);
	}

	private void tagAwareSchedule(Topologies topologies, Cluster cluster) {
		Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();

		// Get the lists of tagged and unreserved supervisors.
		Map<String, ArrayList<SupervisorDetails>> supervisorsByTag = getSupervisorsByTag(supervisorDetails);

		for (TopologyDetails topologyDetails : cluster.needsSchedulingTopologies(topologies)) {
			StormTopology stormTopology = topologyDetails.getTopology();
			String topologyID = topologyDetails.getId();

			// Get components from topology
			Map<String, Bolt> bolts = stormTopology.get_bolts();
			Map<String, SpoutSpec> spouts = stormTopology.get_spouts();

			// Get a map of component to executors
			Map<String, List<ExecutorDetails>> executorsByComponent = cluster
					.getNeedsSchedulingComponentToExecutors(topologyDetails);

			// Get a map of tag to components
			Map<String, ArrayList<String>> componentsByTag = new HashMap<String, ArrayList<String>>();
			populateComponentsByTag(componentsByTag, bolts);
			populateComponentsByTag(componentsByTag, spouts);
			// populateComponentsByTagWithStormInternals(componentsByTag,
			// executorsByComponent.keySet());

			// Get a map of tag to executors
			Map<String, ArrayList<ExecutorDetails>> executorsToBeScheduledByTag = getExecutorsToBeScheduledByTag(
					cluster, topologyDetails, componentsByTag);

			// Initialize a map of slot -> executors
			Map<WorkerSlot, ArrayList<ExecutorDetails>> componentExecutorsToSlotsMap = (new HashMap<WorkerSlot, ArrayList<ExecutorDetails>>());

			// Time to match everything up!
			for (Entry<String, ArrayList<ExecutorDetails>> entry : executorsToBeScheduledByTag.entrySet()) {
				String tag = entry.getKey();

				ArrayList<ExecutorDetails> executorsForTag = entry.getValue();
				if (logger.isDebugEnabled()) {
					logger.debug("tag: " + tag);
				}
				ArrayList<SupervisorDetails> supervisorsForTag = supervisorsByTag.get(tag);
				ArrayList<String> componentsForTag = componentsByTag.get(tag);

				try {
					populateComponentExecutorsToSlotsMap(componentExecutorsToSlotsMap, cluster, topologyDetails,
							supervisorsForTag, executorsForTag, componentsForTag, tag);
				} catch (Exception e) {
					e.printStackTrace();

					// Cut this scheduling short to avoid partial scheduling.
					return;
				}
			}

			// Do the actual assigning We do this as a separate step to only perform any
			// assigning if there have been no issues so far. That's aimed at avoiding
			// partial scheduling from occurring, with some components already scheduled and
			// alive, while others cannot be scheduled.
			for (Entry<WorkerSlot, ArrayList<ExecutorDetails>> entry : componentExecutorsToSlotsMap.entrySet()) {
				WorkerSlot slotToAssign = entry.getKey();
				ArrayList<ExecutorDetails> executorsToAssign = entry.getValue();

				cluster.assign(slotToAssign, topologyID, executorsToAssign);
			}

			// If we've reached this far, then scheduling must have been successful
			cluster.setStatus(topologyID, "SCHEDULING SUCCESSFUL");
		}
	}

	private Map<String, ArrayList<SupervisorDetails>> getSupervisorsByTag(
			Collection<SupervisorDetails> supervisorDetails) {
		// A map of tag -> supervisors, to help with scheduling of components with
		// specific tags
		Map<String, ArrayList<SupervisorDetails>> supervisorsByTag = new HashMap<String, ArrayList<SupervisorDetails>>();

		for (SupervisorDetails supervisor : supervisorDetails) {
			@SuppressWarnings("unchecked")
			Map<String, String> metadata = (Map<String, String>) supervisor.getSchedulerMeta();

			if (logger.isDebugEnabled()) {
				logger.debug("supervisor.getSchedulerMeta(): " + supervisor.getSchedulerMeta());
			}

			String tags;

			if (metadata == null) {
				if (logger.isDebugEnabled()) {
					logger.debug("metadata null");
				}
				tags = untaggedTag;
			} else {
				tags = metadata.get("tags");
				if (logger.isDebugEnabled()) {
					logger.debug("metadata not null: " + tags);
				}
				if (tags == null) {
					if (logger.isDebugEnabled()) {
						logger.debug("tags null");
					}
					tags = untaggedTag;
				}
			}

			// If the supervisor has tags attached to it, handle it by populating the
			// supervisorsByTag map. Loop through each of the tags to handle individually
			if (logger.isDebugEnabled()) {
				logger.debug("tags: " + tags);
			}
			for (String tag : tags.split(",")) {
				tag = tag.trim();

				if (supervisorsByTag.containsKey(tag)) {
					// If we've already seen this tag, then just add the supervisor to the existing
					// ArrayList.
					supervisorsByTag.get(tag).add(supervisor);
				} else {
					// If this tag is new, then create a new ArrayList<SupervisorDetails>, add the
					// current supervisor, and populate the map's tag entry with it.
					ArrayList<SupervisorDetails> newSupervisorList = new ArrayList<SupervisorDetails>();
					newSupervisorList.add(supervisor);
					supervisorsByTag.put(tag, newSupervisorList);
				}
			}
		}
		return supervisorsByTag;
	}

	private <T> void populateComponentsByTag(Map<String, ArrayList<String>> componentsByTag,
			Map<String, T> components) {
		// Type T can be either Bolt or SpoutSpec, so that this logic can be reused for
		// both component types
		JSONParser parser = new JSONParser();

		for (Entry<String, T> componentEntry : components.entrySet()) {
			JSONObject conf = null;

			String componentID = componentEntry.getKey();
			T component = componentEntry.getValue();

			try {
				// Get the component's conf irrespective of its type (via java reflection)
				Method getCommonComponentMethod = component.getClass().getMethod("get_common");
				ComponentCommon commonComponent = (ComponentCommon) getCommonComponentMethod.invoke(component);
				conf = (JSONObject) parser.parse(commonComponent.get_json_conf());
			} catch (ParseException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
				ex.printStackTrace();
			}

			String tags;

			// If there's no config, use a fake tag to group all untagged components
			if (conf == null) {
				if (logger.isDebugEnabled()) {
					logger.debug("conf == null");
				}
				tags = untaggedTag;
			} else {
				if (logger.isDebugEnabled()) {
					logger.debug("conf != null");
				}
				tags = (String) conf.get("tags");

				// If there are no tags, use a fake tag to group all untagged components
				if (tags == null) {
					if (logger.isDebugEnabled()) {
						logger.debug("tags == null");
					}
					tags = untaggedTag;
				}
			}
			if (logger.isDebugEnabled()) {
				logger.debug("populateComponentsByTag tags: " + tags);
			}

			// If the component has tags attached to it, handle it by populating the
			// componentsByTag map. Loop through each of the tags to handle individually
			for (String tag : tags.split(",")) {
				tag = tag.trim();

				if (componentsByTag.containsKey(tag)) {
					// If we've already seen this tag, then just add the component to the existing
					// ArrayList.
					componentsByTag.get(tag).add(componentID);
				} else {
					// If this tag is new, then create a new ArrayList,
					// add the current component, and populate the map's tag entry with it.
					ArrayList<String> newComponentList = new ArrayList<String>();
					newComponentList.add(componentID);
					componentsByTag.put(tag, newComponentList);
				}
			}
		}
	}

	/**
	 * Storm uses some internal components, like __acker. These components are
	 * topology-agnostic and are therefore not accessible through a StormTopology
	 * object. While a bit hacky, this is a way to make sure that we schedule those
	 * components along with our topology ones: we treat these internal components
	 * as regular untagged components and add them to the componentsByTag map.
	 * 
	 * @param componentsByTag
	 * @param components
	 */
	private void populateComponentsByTagWithStormInternals(Map<String, ArrayList<String>> componentsByTag,
			Set<String> components) {

		for (String componentID : components) {
			if (componentID.startsWith("__")) {
				if (logger.isDebugEnabled()) {
					logger.debug("componentID.startsWith(\"__\")");
				}
				if (componentsByTag.containsKey(untaggedTag)) {
					if (logger.isDebugEnabled()) {
						logger.debug("component contains untaggedTag: " + untaggedTag);
					}
					// If we've already seen untagged components, then just add the component to the
					// existing ArrayList.
					componentsByTag.get(untaggedTag).add(componentID);
				} else {
					if (logger.isDebugEnabled()) {
						logger.debug("component does not contain untaggedTag: " + untaggedTag);
					}
					// If this is the first untagged component we see, then create a new ArrayList,
					// add the current component, and populate the map's untagged entry with it.
					ArrayList<String> newComponentList = new ArrayList<String>();
					newComponentList.add(componentID);
					componentsByTag.put(untaggedTag, newComponentList);
				}
			}
		}
	}

	/**
	 * Get the existing assignment of the current topology as it's live in the
	 * cluster
	 * 
	 * @param cluster
	 * @param topologyDetails
	 * @return
	 */
	private Set<ExecutorDetails> getAliveExecutors(Cluster cluster, TopologyDetails topologyDetails) {
		SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyDetails.getId());

		// Return alive executors, if any, otherwise an empty set
		if (existingAssignment != null) {
			return existingAssignment.getExecutors();
		} else {
			return new HashSet<ExecutorDetails>();
		}
	}

	private Map<String, ArrayList<ExecutorDetails>> getExecutorsToBeScheduledByTag(Cluster cluster,
			TopologyDetails topologyDetails, Map<String, ArrayList<String>> componentsPerTag) {
		// Initialise the return value
		Map<String, ArrayList<ExecutorDetails>> executorsByTag = new HashMap<String, ArrayList<ExecutorDetails>>();

		// Find which topology executors are already assigned
		Set<ExecutorDetails> aliveExecutors = getAliveExecutors(cluster, topologyDetails);

		// Get a map of component to executors for the topology that need scheduling
		Map<String, List<ExecutorDetails>> executorsByComponent = cluster
				.getNeedsSchedulingComponentToExecutors(topologyDetails);

		// Loop through componentsPerTag to populate the map
		for (Entry<String, ArrayList<String>> entry : componentsPerTag.entrySet()) {
			String tag = entry.getKey();
			ArrayList<String> componentIDs = entry.getValue();

			// Initialize the map entry for the current tag
			ArrayList<ExecutorDetails> executorsForTag = new ArrayList<ExecutorDetails>();

			// Loop through this tag's component IDs
			for (String componentID : componentIDs) {
				// Fetch the executors for the current component ID
				List<ExecutorDetails> executorsForComponent = executorsByComponent.get(componentID);

				if (executorsForComponent == null) {
					continue;
				}

				// Convert the list of executors to a set
				Set<ExecutorDetails> executorsToAssignForComponent = new HashSet<ExecutorDetails>(
						executorsForComponent);

				// Remove already assigned executors from the set of executors to assign, if any
				executorsToAssignForComponent.removeAll(aliveExecutors);

				// Add the component's waiting to be assigned executors to the current tag
				// executors
				executorsForTag.addAll(executorsToAssignForComponent);
			}

			// Populate the map of executors by tag after looping through all of the tag's
			// components, if there are any executors to be scheduled
			if (!executorsForTag.isEmpty()) {
				executorsByTag.put(tag, executorsForTag);
			}
		}

		return executorsByTag;
	}

	/**
	 * This is the prefix of the message displayed on Storm's UI for any
	 * unsuccessful scheduling
	 * 
	 * @param cluster
	 * @param topologyDetails
	 * @param message
	 * @throws Exception
	 */
	private void handleUnsuccessfulScheduling(Cluster cluster, TopologyDetails topologyDetails, String message)
			throws Exception {
		String unsuccessfulSchedulingMessage = "SCHEDULING FAILED: ";

		cluster.setStatus(topologyDetails.getId(), unsuccessfulSchedulingMessage + message);
		throw new Exception(message);
	}

	/**
	 * Get the existing assignment of the current topology as it's live in the
	 * cluster
	 * 
	 * @param cluster
	 * @param topologyDetails
	 * @return
	 */
	private Set<WorkerSlot> getAliveSlots(Cluster cluster, TopologyDetails topologyDetails) {
		SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyDetails.getId());

		// Return alive slots, if any, otherwise an empty set
		if (existingAssignment != null) {
			return existingAssignment.getSlots();
		} else {
			return new HashSet<WorkerSlot>();
		}
	}

	private List<WorkerSlot> getSlotsToAssign(Cluster cluster, TopologyDetails topologyDetails,
			List<SupervisorDetails> supervisors, List<String> componentsForTag, String tag) throws Exception {
		String topologyID = topologyDetails.getId();

		// Collect the available slots of each of the supervisors we were given in a
		// list
		List<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
		for (SupervisorDetails supervisor : supervisors) {
			if (logger.isDebugEnabled()) {
				logger.debug("Supervisor for tag[" + tag + "]: " + supervisor);
			}
			availableSlots.addAll(cluster.getAvailableSlots(supervisor));
		}

		if (availableSlots.isEmpty()) {
			// This is bad, we have supervisors and executors to assign, but no available
			// slots!
			String message = String.format("No slots are available for assigning executors for tag %s (components: %s)",
					tag, componentsForTag);
			handleUnsuccessfulScheduling(cluster, topologyDetails, message);
		}

		Set<WorkerSlot> aliveSlots = getAliveSlots(cluster, topologyDetails);

		int numAvailableSlots = availableSlots.size();
		int numSlotsNeeded = topologyDetails.getNumWorkers() - aliveSlots.size();

		// We want to check that we have enough available slots
		// based on the topology's number of workers and already assigned slots.
		if (numAvailableSlots < numSlotsNeeded) {
			// This is bad, we don't have enough slots to assign to!
			String message = String.format(
					"Not enough slots available for assigning executors for tag %s (components: %s). "
							+ "Need %s slots to schedule but found only %s",
					tag, componentsForTag, numSlotsNeeded, numAvailableSlots);
			handleUnsuccessfulScheduling(cluster, topologyDetails, message);
		}

		// Now we can use only as many slots as are required.
		return availableSlots.subList(0, numSlotsNeeded);
	}

	private Map<WorkerSlot, ArrayList<ExecutorDetails>> getExecutorsBySlot(List<WorkerSlot> slots,
			List<ExecutorDetails> executors) {
		Map<WorkerSlot, ArrayList<ExecutorDetails>> assignments = new HashMap<WorkerSlot, ArrayList<ExecutorDetails>>();

		int numberOfSlots = slots.size();

		// We want to split the executors as evenly as possible, across each slot
		// available, so we assign each executor to a slot via round robin
		for (int i = 0; i < executors.size(); i++) {
			WorkerSlot slotToAssign = slots.get(i % numberOfSlots);
			ExecutorDetails executorToAssign = executors.get(i);

			if (assignments.containsKey(slotToAssign)) {
				// If we've already seen this slot, then just add the executor to the existing
				// ArrayList.
				assignments.get(slotToAssign).add(executorToAssign);
			} else {
				// If this slot is new, then create a new ArrayList,
				// add the current executor, and populate the map's slot entry with it.
				ArrayList<ExecutorDetails> newExecutorList = new ArrayList<ExecutorDetails>();
				newExecutorList.add(executorToAssign);
				assignments.put(slotToAssign, newExecutorList);
			}
		}

		return assignments;
	}

	private void populateComponentExecutorsToSlotsMap(
			Map<WorkerSlot, ArrayList<ExecutorDetails>> componentExecutorsToSlotsMap, Cluster cluster,
			TopologyDetails topologyDetails, List<SupervisorDetails> supervisors, List<ExecutorDetails> executors,
			List<String> componentsForTag, String tag) throws Exception {
		String topologyID = topologyDetails.getId();

		if (supervisors == null) {
			// This is bad, we don't have any supervisors but have executors to assign!
			String message = String.format(
					"No supervisors given for executors %s of topology %s and tag %s (components: %s)", executors,
					topologyID, tag, componentsForTag);
			handleUnsuccessfulScheduling(cluster, topologyDetails, message);
		}

		List<WorkerSlot> slotsToAssign = getSlotsToAssign(cluster, topologyDetails, supervisors, componentsForTag, tag);

		// Divide the executors evenly across the slots and get a map of slot to
		// executors
		Map<WorkerSlot, ArrayList<ExecutorDetails>> executorsBySlot = getExecutorsBySlot(slotsToAssign, executors);

		for (Entry<WorkerSlot, ArrayList<ExecutorDetails>> entry : executorsBySlot.entrySet()) {
			WorkerSlot slotToAssign = entry.getKey();
			ArrayList<ExecutorDetails> executorsToAssign = entry.getValue();

			// Assign the topology's executors to slots in the cluster's supervisors
			componentExecutorsToSlotsMap.put(slotToAssign, executorsToAssign);
		}
	}
}
