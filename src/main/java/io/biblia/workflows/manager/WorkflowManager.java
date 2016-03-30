package io.biblia.workflows.manager;

import io.biblia.workflows.definition.Workflow;

public interface WorkflowManager {

	/**
	 * Submits a workflow to be ran.
	 * @param workflow
	 * @return
	 */
	public String submitWorkflow(Workflow workflow);
}
