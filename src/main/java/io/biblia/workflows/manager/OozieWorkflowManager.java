package io.biblia.workflows.manager;

import io.biblia.workflows.definition.Workflow;

/**
 * Uses Oozie to submit the tasks of a workflow to Hadoop.
 * @author jadiel
 *
 */
public class OozieWorkflowManager implements WorkflowManager {

	@Override
	public String submitWorkflow(Workflow workflow) {
		
		//1. Store workflow and attach a unique id to the workflow (from the database)
		
		//2. Submit to Oozie the actions of the workflow that have no parents.
		
		//3. 
		return null;
	}

}
