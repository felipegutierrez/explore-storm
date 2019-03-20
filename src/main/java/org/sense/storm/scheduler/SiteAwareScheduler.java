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
import org.sense.storm.utils.TagSite;

public class SiteAwareScheduler implements IScheduler {

	final static Logger logger = Logger.getLogger(SiteAwareScheduler.class);

	@Override
	public void prepare(Map conf) {

	}

	/**
	 * Schedules all topologies on the cluster.
	 * 
	 * <pre>
	 * 1 - Get the lists of tagged and unreserved supervisors.
	 * 2 - Get all topologies that have to be schedule on the cluster. The method cluster.needsSchedulingTopologies() does that.
	 * 3 - Then, we look for all SPOUTS and BOLTS on this topology. We create an object "Map<String, ArrayList<String>> componentsByMeta" to handle 
	 * the metadata of the SPOUTS and BOLTS.
	 * Then we need all the executors (threads) available on the worker nodes. The worker nodes are the ones that execute our tasks on the cluster.
	 * We handle the executors together with the WorkerSlot in a Map. One WorkerSlot can have several Executors. This is handle on the object:
	 * "Map<WorkerSlot, ArrayList<ExecutorDetails>> componentExecutorsToSlotsMap".
	 * 4 - Get a map of tag to executors and its corresponding metadata written by the programmer.
	 * 5 - To schedule a task on the cluster we need to find a WorkerSlot and add an Executor (thread) on it. WorkerSlots can handle more 
	 * than one executors (threads). We create this object with the name "Map<WorkerSlot, ArrayList<ExecutorDetails>> componentExecutorsToSlotsMap"
	 * and we iterate over it on the step 9 in order to assign the WorkerSlot's to the cluster.
	 * 6 - Time to match everything up!
	 * 7 - Divide the executors evenly across the slots and get a map of slot->executors. This is hold on the object 
	 * "Map<WorkerSlot, ArrayList<ExecutorDetails>> executorsBySlot". We want to split the executors as evenly as possible, across each slot available, 
	 * so we assign each executor to a slot via round robin
	 * 8 - Assign the topology's executors to slots in the cluster's supervisors
	 * 9 - Do the actual assigning We do this as a separate step to only perform any assigning if there have been no issues so far. 
	 * That's aimed at avoiding partial scheduling from occurring, with some components already scheduled and alive, 
	 * while others cannot be scheduled.
	 * 10 - the topology was successfully scheduled.
	 * </pre>
	 */
	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		try {
			// 1
			Collection<SupervisorDetails> supervisorDetails = cluster.getSupervisors().values();
			Map<String, ArrayList<SupervisorDetails>> supervisorsByMeta = getSupervisorsByMeta(supervisorDetails);

			// 2
			List<TopologyDetails> topologiesToBeSchedule = cluster.needsSchedulingTopologies(topologies);
			for (TopologyDetails topologyDetails : topologiesToBeSchedule) {

				// 3
				String topologyID = topologyDetails.getId();
				Map<String, Bolt> bolts = topologyDetails.getTopology().get_bolts();
				Map<String, SpoutSpec> spouts = topologyDetails.getTopology().get_spouts();

				// Get a map of (component-id -> executors) which needs scheduling
				// Map<String, List<ExecutorDetails>> executorsByComponent =
				// cluster.getNeedsSchedulingComponentToExecutors(topologyDetails);

				Map<String, ArrayList<String>> componentsByMetadata = new HashMap<String, ArrayList<String>>();
				populateComponentsByMetadata(componentsByMetadata, bolts);
				populateComponentsByMetadata(componentsByMetadata, spouts);
				// populateComponentsByTagWithStormInternals(componentsByTag,executorsByComponent.keySet());

				// 4
				Map<String, ArrayList<ExecutorDetails>> executorsToBeScheduledByMeta = getExecutorsToBeScheduledBySite(
						cluster, topologyDetails, componentsByMetadata);

				// 5
				Map<WorkerSlot, ArrayList<ExecutorDetails>> componentExecutorsToSlotsMap = new HashMap<WorkerSlot, ArrayList<ExecutorDetails>>();

				// 6
				for (Entry<String, ArrayList<ExecutorDetails>> executorDetails : executorsToBeScheduledByMeta
						.entrySet()) {
					String executorMetadataKey = executorDetails.getKey();
					ArrayList<ExecutorDetails> executorsForSite = executorDetails.getValue();
					ArrayList<SupervisorDetails> supervisorsForSite = supervisorsByMeta.get(executorMetadataKey);
					ArrayList<String> componentsForSite = componentsByMetadata.get(executorMetadataKey);

					if (supervisorsForSite == null) {
						// This is bad, we don't have any supervisors but have executors to assign!
						String message = String.format(
								"No supervisors given for executors %s of topology %s and metadata %s (components: %s)",
								executorsForSite, topologyID, executorMetadataKey, componentsForSite);
						handleUnsuccessfulScheduling(cluster, topologyDetails, message);
					}

					List<WorkerSlot> slotsToAssign = getSlotsToAssign(cluster, topologyDetails, supervisorsForSite,
							componentsForSite, executorMetadataKey);

					// 7
					Map<WorkerSlot, ArrayList<ExecutorDetails>> executorsByWorkerSlot = new HashMap<WorkerSlot, ArrayList<ExecutorDetails>>();
					int numberOfSlots = slotsToAssign.size();
					System.out.println("slotsToAssign.size(): " + slotsToAssign.size());
					for (int i = 0; i < executorsForSite.size(); i++) {
						WorkerSlot slotToAssign = slotsToAssign.get(i % numberOfSlots);
						ExecutorDetails executorToAssign = executorsForSite.get(i);
						if (executorsByWorkerSlot.containsKey(slotToAssign)) {
							executorsByWorkerSlot.get(slotToAssign).add(executorToAssign);
						} else {
							ArrayList<ExecutorDetails> newExecutorList = new ArrayList<ExecutorDetails>();
							newExecutorList.add(executorToAssign);
							executorsByWorkerSlot.put(slotToAssign, newExecutorList);
						}
					}

					// 8
					for (Entry<WorkerSlot, ArrayList<ExecutorDetails>> workerSlotAndExecutors : executorsByWorkerSlot
							.entrySet()) {
						WorkerSlot workerSlotToAssign = workerSlotAndExecutors.getKey();
						ArrayList<ExecutorDetails> executorsToAssign = workerSlotAndExecutors.getValue();
						componentExecutorsToSlotsMap.put(workerSlotToAssign, executorsToAssign);
					}
				}

				// 9
				for (Entry<WorkerSlot, ArrayList<ExecutorDetails>> entry : componentExecutorsToSlotsMap.entrySet()) {
					WorkerSlot workerSlotToAssign = entry.getKey();
					ArrayList<ExecutorDetails> executorsToAssign = entry.getValue();
					cluster.assign(workerSlotToAssign, topologyID, executorsToAssign);
				}
				// 10
				cluster.setStatus(topologyID, "SCHEDULING SUCCESSFUL for topology " + topologyID);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	/**
	 * get a Map of all supervisors available on the cluster. Once the file
	 * "conf/storm.yaml" has been configured with the metadata "storm.scheduler" for
	 * the extendable Scheduler and the metadata "supervisor.scheduler.meta" which
	 * defines the metadata for each supervisor ,the method getSchedulerMeta() reads
	 * the metadata for each supervisor in order to schedule the topology over them.
	 * 
	 * @param supervisorDetails
	 * @return
	 */
	private Map<String, ArrayList<SupervisorDetails>> getSupervisorsByMeta(
			Collection<SupervisorDetails> supervisorDetails) {
		Map<String, ArrayList<SupervisorDetails>> supervisorsByMetadata = new HashMap<String, ArrayList<SupervisorDetails>>();

		for (SupervisorDetails supervisor : supervisorDetails) {
			Map<String, String> metadata = (Map<String, String>) supervisor.getSchedulerMeta();

			String metadataValue;
			if (metadata == null) {
				logger.error("metadata cannot be null!");
				metadataValue = TagSite.UNTAGGED.getValue();
			} else {
				metadataValue = metadata.get(TagSite.SITE.getValue());
				if (metadataValue == null) {
					logger.error(TagSite.SITE.getValue() + " cannot be null!");
					metadataValue = TagSite.UNTAGGED.getValue();
				}
			}

			metadataValue = metadataValue.trim();
			if (supervisorsByMetadata.containsKey(metadataValue)) {
				supervisorsByMetadata.get(metadataValue).add(supervisor);
			} else {
				ArrayList<SupervisorDetails> newSupervisorList = new ArrayList<SupervisorDetails>();
				newSupervisorList.add(supervisor);
				supervisorsByMetadata.put(metadataValue, newSupervisorList);
			}
		}
		return supervisorsByMetadata;
	}

	/**
	 * 
	 * @param componentsByMetadata
	 * @param components
	 */
	private <T> void populateComponentsByMetadata(Map<String, ArrayList<String>> componentsByMetadata,
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

			String metadata;
			if (conf == null) {
				metadata = TagSite.UNTAGGED.getValue();
			} else {
				metadata = (String) conf.get(TagSite.SITE.getValue());
				if (metadata == null) {
					metadata = TagSite.UNTAGGED.getValue();
				}
			}
			System.out.println("populateComponentsByMetadata: " + metadata);

			// If the component has tags attached to it, handle it by populating the
			// componentsByTag map. Loop through each of the tags to handle individually
			metadata = metadata.trim();
			if (componentsByMetadata.containsKey(metadata)) {
				componentsByMetadata.get(metadata).add(componentID);
				System.out.println("add: " + componentID);
			} else {
				ArrayList<String> newComponentList = new ArrayList<String>();
				newComponentList.add(componentID);
				componentsByMetadata.put(metadata, newComponentList);
				System.out.println("add: " + componentID);
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
				logger.debug("componentID.startsWith(\"__\")");
				if (componentsByTag.containsKey(TagSite.UNTAGGED.getValue())) {
					logger.debug("component contains untaggedTag: " + TagSite.UNTAGGED.getValue());
					// If we've already seen untagged components, then just add the component to the
					// existing ArrayList.
					componentsByTag.get(TagSite.UNTAGGED.getValue()).add(componentID);
				} else {
					logger.debug("component does not contain untaggedTag: " + TagSite.UNTAGGED.getValue());
					// If this is the first untagged component we see, then create a new ArrayList,
					// add the current component, and populate the map's untagged entry with it.
					ArrayList<String> newComponentList = new ArrayList<String>();
					newComponentList.add(componentID);
					componentsByTag.put(TagSite.UNTAGGED.getValue(), newComponentList);
				}
			}
		}
	}

	/**
	 * This method get all the executors from the topology that will be scheduled on
	 * the cluster. The Executors are the transformations from the topology which
	 * will be a thread on the execution environment. These executors are created
	 * using the "addConfiguration('site', 'cluster|edge')" method in order to set
	 * the location to compute the task.
	 * 
	 * @param cluster
	 * @param topologyDetails
	 * @param componentsByMetadata
	 * @return
	 */
	private Map<String, ArrayList<ExecutorDetails>> getExecutorsToBeScheduledBySite(Cluster cluster,
			TopologyDetails topologyDetails, Map<String, ArrayList<String>> componentsByMetadata) {
		System.out.println("4 - getExecutorsToBeScheduledBySite");
		// This is the list of executors (site -> executors). A executor is a thread
		// that runs inside the Worker Process.
		Map<String, ArrayList<ExecutorDetails>> executorsBySite = new HashMap<String, ArrayList<ExecutorDetails>>();

		// Find which topology executors are already assigned
		Set<ExecutorDetails> aliveExecutors = null;
		SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyDetails.getId());
		if (existingAssignment != null) {
			aliveExecutors = existingAssignment.getExecutors();
		} else {
			aliveExecutors = new HashSet<ExecutorDetails>();
		}

		// Get a map of (component-id -> executors) which needs scheduling
		Map<String, List<ExecutorDetails>> executorsByComponent = cluster
				.getNeedsSchedulingComponentToExecutors(topologyDetails);

		// Iterate over the componentsPerTag created before in order to populate the
		// executorsByTag
		for (Entry<String, ArrayList<String>> metadataComponent : componentsByMetadata.entrySet()) {
			String metadataKey = metadataComponent.getKey();
			ArrayList<ExecutorDetails> executorsForTag = new ArrayList<ExecutorDetails>();
			System.out.println("metadataKey: " + metadataKey);

			for (String metadataValue : metadataComponent.getValue()) {
				// Fetch the executors for the current component ID
				List<ExecutorDetails> executorsForComponent = executorsByComponent.get(metadataValue);
				System.out.println(
						"Get list of ExecutorDetails with metadata[" + metadataValue + "]: " + executorsForComponent);

				if (executorsForComponent == null) {
					continue;
				}
				System.out.println("size: " + executorsForComponent.size());

				// Convert the list of executors to a set
				Set<ExecutorDetails> executorsToAssignForComponent = new HashSet<ExecutorDetails>(
						executorsForComponent);

				// Remove already assigned executors from the set of executors to assign, if any
				executorsToAssignForComponent.removeAll(aliveExecutors);

				// Add the component's waiting to be assigned executors to the current tag
				// executors
				if (!executorsToAssignForComponent.isEmpty()) {
					executorsForTag.addAll(executorsToAssignForComponent);
				}
			}

			// Populate the map of executors by tag after looping through all of the tag's
			// components, if there are any executors to be scheduled
			if (!executorsForTag.isEmpty()) {
				executorsBySite.put(metadataKey, executorsForTag);
			}
		}
		return executorsBySite;
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

	private List<WorkerSlot> getSlotsToAssign(Cluster cluster, TopologyDetails topologyDetails,
			List<SupervisorDetails> supervisors, List<String> componentsForSite, String site) throws Exception {

		// Collect the available slots of each of the supervisors we were given in a
		// list
		List<WorkerSlot> availableSlots = new ArrayList<WorkerSlot>();
		for (SupervisorDetails supervisor : supervisors) {
			System.out.println("Supervisor for site[" + site + "]: " + supervisor);
			availableSlots.addAll(cluster.getAvailableSlots(supervisor));
		}

		if (availableSlots.isEmpty()) {
			// This is bad, we have supervisors and executors to assign, but no available
			// slots!
			String message = String.format(
					"No slots are available for assigning executors for site %s (components: %s)", site,
					componentsForSite);
			handleUnsuccessfulScheduling(cluster, topologyDetails, message);
		}

		Set<WorkerSlot> aliveSlots = null;
		SchedulerAssignment existingAssignment = cluster.getAssignmentById(topologyDetails.getId());
		if (existingAssignment != null) {
			aliveSlots = existingAssignment.getSlots();
		} else {
			aliveSlots = new HashSet<WorkerSlot>();
		}

		int numAvailableSlots = availableSlots.size();
		int numSlotsNeeded = topologyDetails.getNumWorkers() - aliveSlots.size();

		// We want to check that we have enough available slots
		// based on the topology's number of workers and already assigned slots.
		if (numAvailableSlots < numSlotsNeeded) {
			// This is bad, we don't have enough slots to assign to!
			String message = String.format(
					"Not enough slots available for assigning executors for site %s (components: %s). "
							+ "Need %s slots to schedule but found only %s",
					site, componentsForSite, numSlotsNeeded, numAvailableSlots);
			handleUnsuccessfulScheduling(cluster, topologyDetails, message);
		}

		// Now we can use only as many slots as are required.
		return availableSlots.subList(0, numSlotsNeeded);
	}
}
