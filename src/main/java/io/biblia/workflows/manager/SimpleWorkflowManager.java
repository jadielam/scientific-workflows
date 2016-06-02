package io.biblia.workflows.manager;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;

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
		Queue<Integer> actionsQueue = new ArrayDeque<>();
		Set<Action> alreadyComputed = new HashSet<>();
		actionsQueue.addAll(this.getActionsWithNoParents(workflow));
		while (!actionsQueue.isEmpty()) {
			Integer nextActionId = actionsQueue.poll();
			Action action = workflow.getAction(nextActionId);
			Collection<Action> childActions = workflow.getChildActions(nextActionId);
			List<Integer> childActionsIds = new ArrayList<Integer>();
			for (Action childAction : childActions) {
				childActionsIds.add(childAction.getActionId());
			}
			actionsQueue.addAll(childActionsIds);
			String outputPath = action.getOutputPath();
			try {
				PersistedDataset dataset = this.dPersistance.getDatasetByPath(outputPath);
				DatasetState state = dataset.getState();
				if (state.equals(DatasetState.STORED)) {
					
				}
			} catch (DatasetParseException e) {
				e.printStackTrace();
				continue;
			}
			
		}
		
		//1. For each intermediate action, determine if the datasets
		//will be deleted.
		
		//2. Determine which actions do not need to be computed.		
		//3. Add claims to datasets for all the actions that will use
		//them as input
		
		//Steps 2 and 3 need to be performed together, synchronizing the
		//database access to those datasets by changing them to an intermediate
		//state. That is, I can only determine that an action does not
		//need to be computed after I have changed the state of a dataset
		//to LOCKED (or any other state) with success. After I have been able
		//to successfully change the state of the dataset, then I will
		//decide to place a claim on it by an action.
		//I will need to store in the action all the datasets on which I have
		//a claim, so that when I do a callback, I reduce the claim counter
		//on those datasets.
		
		//4. 
		return null;
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
