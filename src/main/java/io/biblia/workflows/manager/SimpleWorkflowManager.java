package io.biblia.workflows.manager;

import io.biblia.workflows.definition.Workflow;

public class SimpleWorkflowManager implements WorkflowManager {

	@Override
	public String submitWorkflow(Workflow workflow) {

		// TODO Auto-generated method stub
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
		return null;
	}

}
