package io.biblia.workflows.manager;

import java.util.ArrayDeque;

import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Deque;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

import org.bson.types.ObjectId;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.Workflow;
import io.biblia.workflows.definition.parser.DatasetParseException;
import io.biblia.workflows.manager.action.ActionPersistance;
import io.biblia.workflows.manager.dataset.DatasetPersistance;
import io.biblia.workflows.manager.dataset.DatasetState;
import io.biblia.workflows.manager.dataset.PersistedDataset;

public class SimpleWorkflowManager implements WorkflowManager {

	private final DatasetPersistance dPersistance;
	private final ActionPersistance aPersistance;
	
	public SimpleWorkflowManager(DatasetPersistance dPersistance,
			ActionPersistance aPersistance) {
		this.dPersistance = dPersistance;
		this.aPersistance = aPersistance;
		
	}
	
	@Override
	public String submitWorkflow(Workflow workflow) {

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
			List<Integer> parents = action.getParentIds();
			if (null == parents || parents.size() == 0) {
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
			if (!next.isManaged() && !next.forceComputation()) {
				
				//1.4.2.1 Get dataset corresponding to this action
				String actionFolder = next.getActionFolder();
				try{
					PersistedDataset dataset = this.dPersistance.getDatasetByPath(actionFolder);
					if (null == dataset || dataset.getState().equals(DatasetState.DELETED)
							|| dataset.getState().equals(DatasetState.DELETING)
							|| dataset.getState().equals(DatasetState.PROCESSING)
							|| dataset.getState().equals(DatasetState.TO_DELETE)) {
					
						prepareForComputation(next, actionsToCompute, processedActions, Q, workflow);
					}
					
					
					else {
						
						//1.4.2.1 If the dataset exists and is in state STORED or LEAF:
						//1.4.2.1.1 If you are a LEAF and dataset state is STORED, change-force the 
						//dataset state to be LEAF.
						//1.4.2.1.2 For all your children who were added to the map of newly added
						//actions that need to be computed, add a claim to that dataset from each
						//of those children.  If the claim fails because the dataset was updated
						//previously, then keep adding the claim until you succeed, unless
						//the dataset state has been changed to TO_DELETE or PROCESSING or DELETING
						//or DELETED, in which case we do the logic of 1.4.2.2
					}
				}
				catch(DatasetParseException e) {
					prepareForComputation(next, actionsToCompute, processedActions, Q, workflow);
				}
				
				
			}
			
			//If the action is MANAGE_YOURSELF or FORCE_COMPUTATION
			else {
				prepareForComputation(next, actionsToCompute, processedActions, Q, workflow);
			}
			
			
		
		}
			
		//2. For each intermediate action, determine if the datasets
		//will be deleted using the decision algorithm for it.
				
		return null;
	}
	

	private void prepareForComputation(Action action, Map<Integer, String> actionsToCompute,
			Set<Integer> processedActions, Deque<Action> Q, Workflow workflow) {
		
		//1.4.2.2 If the dataset does not exist, or it is in state TO_DELETE,
		//PROCESSING, DELETING or DELETED, 
		//1.4.2.2.1 Submit the action to MongoDB to get its objectID. The 
		//state of the action is WAITING.
		String databaseId = this.aPersistance.insertWaitingAction(action, Collections.<String>emptyList());
		
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
					//TODO: Add piece method that allows to add pending parents to a
					//WAITING action.
					this.aPersistance.addParentIdToAction(childId, databaseId);
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
	
	private List<Integer> getActionsWithNoParents(Workflow workflow) {
		List<Integer> toReturn = new ArrayList<>();
		for (Action action : workflow.getActions()) {
			if (action.getParentIds().size() == 0) {
				toReturn.add(action.getActionId());
			}
		}
		return toReturn;
	}

}
