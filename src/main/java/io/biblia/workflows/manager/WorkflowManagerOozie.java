package io.biblia.workflows.manager;

import io.biblia.workflows.definition.Workflow;

public class WorkflowManagerOozie implements WorkflowManager {

	@Override
	public String submitWorkflow(Workflow workflow) {
		
		// 1. Convert the Workflow to a Oozie workflow.
		// 1. Generate the xml file
		// 2. Generate the properties file
		
		// 2. Submit the workflow using an Oozie client.
		
		// 3. Get back the id of the workflow submitted to Oozie.
		return null;
	}


	
}
