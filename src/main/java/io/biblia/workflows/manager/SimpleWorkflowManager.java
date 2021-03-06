package io.biblia.workflows.manager;

import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Deque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.HashSet;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.Workflow;
import io.biblia.workflows.definition.parser.DatasetParseException;
import io.biblia.workflows.manager.action.ActionPersistance;
import io.biblia.workflows.manager.dataset.DatasetPersistance;
import io.biblia.workflows.manager.dataset.DatasetState;
import io.biblia.workflows.manager.dataset.PersistedDataset;
import io.biblia.workflows.manager.dataset.OutdatedDatasetException;

public class SimpleWorkflowManager implements WorkflowManager {

	private final DatasetPersistance dPersistance;
	private final ActionPersistance aPersistance;
	
	public SimpleWorkflowManager(DatasetPersistance dPersistance,
			ActionPersistance aPersistance) {
		this.dPersistance = dPersistance;
		this.aPersistance = aPersistance;
		
	}
	
	@Override
	public Long submitWorkflow(Workflow workflow) {
		
		//0. Workflow id
		Long workflowId = this.aPersistance.getNextWorkflowSequence();
		
		//1. Determine which actions do not need to be computed.
		//1.1 Create a queue Q that will have actions to be processed.	
		Deque<Action> Q = new LinkedList<Action>();
		//We will add to the set each time we add a new element to the queue.
		Set<Integer> processedActions = new HashSet<>();
		
		//1.2 Create a map M from actionWorkflowId to objectId that will 
		//hold all the new actions that need to be computed.
		Map<Integer, String> actionsToCompute = new HashMap<>();
		
		//1.3 Find all the leaves of the workflow and add them to Q.
		Collection<Action> actions = workflow.getActions();
		for (Action action : actions) {
			if (isLeaf(action, workflow)) {
				Q.addLast(action);
				processedActions.add(action.getActionId());
			}
		}
		
		//1.4 While queue is not empty:
		while(!Q.isEmpty()) {
		
			//1.4.1. Get the next action in the queue.
			Action next = Q.pollFirst();
			
			//1.4.2. If the action is not MANAGE_YOURSELF or FORCE_COMPUTATION
			//get its corresponding dataset.
			if (next.isManaged() && !next.forceComputation()) {
				
				//1.4.2.1 Get dataset corresponding to this action
				String actionOutputPath = next.getOutputPath();
				try{
					PersistedDataset dataset = this.dPersistance.getDatasetByPath(actionOutputPath);
					//1.4.2.1.1 If the dataset exists, and it is in state STORED_TO_DELETE,
					//PROCESSING, DELETING, DELETED; or if dataset does not exists.
					if (null == dataset || dataset.getState().equals(DatasetState.DELETED)
							|| dataset.getState().equals(DatasetState.DELETING)
							|| dataset.getState().equals(DatasetState.PROCESSING)
							|| dataset.getState().equals(DatasetState.TO_DELETE)
							|| dataset.getState().equals(DatasetState.STORED_TO_DELETE)) {
					
						prepareForComputation(next, actionsToCompute, processedActions, Q, workflow, workflowId);
					}
					
					
					else {
						
						//1.4.2.1.2 If the dataset exists and is in state STORED or LEAF:
						if (dataset.getState().equals(DatasetState.STORED)
								|| dataset.getState().equals(DatasetState.LEAF)) {
							
							//1.4.2.1.2.1 If you are a LEAF and dataset state is STORED, change-force the 
							//dataset state to be LEAF.
							if (isLeaf(next, workflow) && dataset.getState().equals(DatasetState.STORED)) {
								
								while (true) {
									try{
										dataset = this.dPersistance.updateDatasetState(dataset, DatasetState.LEAF);
										break;
									}
									catch(OutdatedDatasetException e) {
										dataset = this.dPersistance.getDatasetByPath(actionOutputPath);
										if (null == dataset 
											|| !dataset.getState().equals(DatasetState.LEAF)
											|| !dataset.getState().equals(DatasetState.STORED)) {
										
											//The dataset has changed in a way I cannot handle.
											//so I most compute
											prepareForComputation(next, actionsToCompute, processedActions, Q, workflow, workflowId);
											break;
										}
									}
								}
							}
							
							
							//1.4.2.1.2.2 For all your children who were added to the map of newly added
							//actions that need to be computed, add a claim to that dataset from each
							//of those children.  If the claim fails because the dataset was updated
							//previously, then keep adding the claim until you succeed, unless
							//the dataset state has been changed to TO_DELETE or PROCESSING or DELETING
							//or DELETED, in which case we do the logic of 1.4.2.2
							
							//The execution of this code here is interesting.  Suppose that in the previous step the
							//dataset was sent to prepareForComputation. That means that the dataset is no longer
							//available (it was marked for deletion or is in the process of being deleted).
							//Here I am placing a claim to that dataset. The claim will go without effect, because
							//the purpose of a claim is to stop a dataset from being deleted.  So, I will
							//leave this code as it is.  TODO: If I want to change something here, then I have to
							//stop this from happening if I call prepare for computation above.
							Collection<Action> childActions = workflow.getChildActions(next.getActionId());
							for (Action action : childActions) {
								Integer actionWorkflowId = action.getActionId();
								if (actionsToCompute.containsKey(actionWorkflowId)) {
									String databaseId = actionsToCompute.get(actionWorkflowId);
									
									while (true) {
										try {
											dataset = this.dPersistance.addClaimToDataset(dataset, databaseId);
											break;
										}
										catch(OutdatedDatasetException ex) {
											dataset = this.dPersistance.getDatasetByPath(actionOutputPath);
											if (null == dataset
												|| (!dataset.getState().equals(DatasetState.LEAF)
												&& !dataset.getState().equals(DatasetState.STORED))) {
												
												prepareForComputation(next, actionsToCompute, processedActions, Q, workflow, workflowId);
												break;
											}
										}
										
									}
									
								}
							}
							
						}
						
						//TODO: 1.4.2.1.3 If the dataset exists and is in state TO_STORE, TO_LEAF,
						//then find the action id that is responsible of creating this dataset,
						//and make all the children of the current action to be depending on 
						//the action that is currently computing that dataset. Place a lock on 
						//that dataset so that no one can change it until you are finished updating
						//children dependencies. Also add a claim to that dataset from all those children.
						else if (DatasetState.TO_STORE.equals(dataset.getState())
								 || DatasetState.TO_LEAF.equals(dataset.getState())) {
							//For now, in order to limit complexity, I will just recompute the dataset.
							prepareForComputation(next, actionsToCompute, processedActions, Q, workflow, workflowId);
							//TODO: The current solution is not lacking troubles too. Suppose the following
							//situation: By the time we finish this computation, the dataset already exists
							//in state STORED.  If we overwrite it? How does that affect people
							//reading from that data at the moment?
							//Need to figure out what Hadoop would do.
						}
					}
				}
				catch(DatasetParseException e) {
					prepareForComputation(next, actionsToCompute, processedActions, Q, workflow, workflowId);
				}
				
				
			}
			
			//1.4.3
			//If the action is MANAGE_YOURSELF or FORCE_COMPUTATION
			else {
				prepareForComputation(next, actionsToCompute, processedActions, Q, workflow, workflowId);
			}
		
		}
			
		//TODO: QUestion to answer: How am I adding claims to datasets already/previously existing in the 
				//system, and hence not being computed by me?
		
		//1. For all the actions TO BE COMPUTED in the current submitted workflow, create
		//a new dataset in the database with state of either TO_LEAF or TO_STORE
		Set<Entry<Integer, String>> entrySet = actionsToCompute.entrySet();
		for (Entry<Integer, String> entry : entrySet) {
			Integer actionWorkflowId = entry.getKey();
			Action action = workflow.getAction(actionWorkflowId);
			String datasetPath = action.getOutputPath();
			
			DatasetState state = null;
			if (isLeaf(action, workflow)) {
				state = DatasetState.TO_LEAF;
			}
			else {
				state = DatasetState.TO_STORE;
			}
			PersistedDataset newDataset = new PersistedDataset(datasetPath,
					state, new Date(), 1, Collections.<String>emptyList());
			this.dPersistance.insertDataset(newDataset);
		}
		
		//3. For each action in the map of actions to be computed, if the 
		//action does not have parent actions on which it depends, 
		//change it from a WAITING action to a READY action.
		Set<String> computedActionsDatabaseIds = new HashSet<>(actionsToCompute.values());
		for (String databaseId : computedActionsDatabaseIds) {
		
			this.aPersistance.readyAction(databaseId);
		}
		
		//4. For each action not to be computed, add an entry in the collection of
		//actions saying that this was an action that was previously computed
		Set<Integer> notToComputeActions = new HashSet<Integer>(processedActions);
		notToComputeActions.removeAll(actionsToCompute.keySet());
		
		Set<Integer> insertedComputedActions = new HashSet<Integer>();
		for (Integer actionIdInWorkflow : notToComputeActions) {
			Action action = workflow.getAction(actionIdInWorkflow);
			List<String> parentActionOutputs = workflow.getParentActions(actionIdInWorkflow).stream().
														map(Action::getOutputPath).
														collect(Collectors.toCollection(ArrayList::new));
			this.aPersistance.insertComputedAction(action, workflowId, Collections.<String>emptyList(), parentActionOutputs);
			insertedComputedActions.add(actionIdInWorkflow);
		}
				
		return workflowId;
	}
	
	private boolean isLeaf(Action action, Workflow workflow) {
		Integer actionId = action.getActionId();
		Collection<Action> childs = workflow.getChildActions(actionId);
		if (null == childs || childs.size() == 0) {
			return true;
		}
		return false;
	}

	private void prepareForComputation(Action action, Map<Integer, String> actionsToCompute,
			Set<Integer> processedActions, Deque<Action> Q, Workflow workflow, Long workflowId) {
		
		//1.4.2.2.1 Submit the action to MongoDB to get its objectID. The 
		//state of the action is WAITING.
		
		List<String> parentActionOutputs = workflow.getChildActions(action.getActionId()).
													stream().map(Action::getOutputPath).
													collect(Collectors.toCollection(ArrayList::new));
		
		String databaseId = this.aPersistance.insertWaitingAction(action, workflowId, Collections.<String>emptyList(),
				parentActionOutputs);
		
		
		//1.4.2.2.2 Add this action to the map of actions to be computed.
		actionsToCompute.put(action.getActionId(), databaseId);
		
		//1.4.2.2.3 For each of the children of the action that are on the map
		//of actions to be computed, add the id of this action as a parent to 
		//the child action in the database.
		Collection<Action> childs = workflow.getChildActions(action.getActionId());
		if (null != childs) {
			for (Action child : childs) {
				Integer childId = child.getActionId();
				if (actionsToCompute.containsKey(childId)) {
					
					String childDatabaseId = actionsToCompute.get(childId);
					this.aPersistance.addParentIdToAction(childDatabaseId, databaseId);
				}
			}
		}
		
		//1.4.2.2.4 Add all the parents of the currently added action to the queue
		//if those parents have not already being processed and sent to the queue.
		List<Integer> parentActions = action.getParentIds();
		if (null != parentActions) {
			for (Integer parentId : parentActions) {
				if (!processedActions.contains(parentId)) {
					Q.addLast(workflow.getAction(parentId));
					processedActions.add(parentId);
				}
			}
		}
		
	}
}
