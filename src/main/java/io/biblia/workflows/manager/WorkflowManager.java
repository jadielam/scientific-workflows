package io.biblia.workflows.manager;

import io.biblia.workflows.definition.Workflow;

public interface WorkflowManager {

	/**
	 * Submits a workflow to be ran.  
	 * @param workflow
	 * @return Returns the id of the workflow submitted.
	 */
	public String submitWorkflow(Workflow workflow);
}
