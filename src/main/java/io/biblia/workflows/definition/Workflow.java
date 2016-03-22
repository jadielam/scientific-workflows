package io.biblia.workflows.definition;

import java.util.List;

public class Workflow {

	private final String startAction;
	private final String endAction;
	private final List<Action> actions;
	
	public Workflow(String startAction, String endAction, List<Action> actions) {
		
		this.startAction = startAction;
		this.endAction = endAction;
		this.actions = actions;
	}
	
	private void validateWorkflow(String startAction, String endAction, List<Action> actions) 
		 {
		
	}
}
