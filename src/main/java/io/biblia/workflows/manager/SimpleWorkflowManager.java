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
		//1.1 Create a queue Q that will have actions to be processed.
		//1.2 Create a map M from actionWorkflowId to objectId that will 
		//hold all the new actions that need to be computed.
		//1.3 Find all the leaves of the workflow and add them to Q.
		//1.4 While queue is not empty:
		//1.4.1. Get the next action in the queue.
		//1.4.2. If the action is not MANAGE_YOURSELF or FORCE_COMPUTATION
		//get its corresponding dataset. 
		
		//1.4.2.1 If the dataset exists and is in state STORED or LEAF:
		//1.4.2.1.1 If you are a LEAF and dataset state is STORED, change-force the 
		//dataset state to be LEAF.
		//1.4.2.1.2 For all your children who were added to the map of newly added
		//actions that need to be computed, add a claim to that dataset from each
		//of those children.  If the claim fails because the dataset was updated
		//previously, then keep adding the claim until you succeed, unless
		//the dataset state has been changed to TO_DELETE or PROCESSING or DELETING
		//or DELETED, in which case we do the logic of 1.4.2.2
		
		//1.4.2.2 If the dataset does not exist, or it is in state TO_DELETE,
		//PROCESSING, DELETING or DELETED, 
		//1.4.2.2.1 Submit the action to MongoDB to get its objectID. The 
		//state of the action is WAITING.
		//1.4.2.2.2 Add this action to the map of actions to be computed.
		//1.4.2.2.3 For each of the children of the action that are on the map
		//of actions to be computed, add the id of this action as a parent to 
		//the child action in the database.
		//1.4.2.2.4 Add all the parents of the currently added action to the queue
		//if those parents have not already being processed and sent to the queue.
		
		//2. For each intermediate action, determine if the datasets
		//will be deleted using the decision algorithm for it.
				
		
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
