package io.biblia.workflows.manager.action;

import com.google.common.base.Preconditions;

public class ActionSubmitter implements Runnable {
	
	private final ProcessingAction action;
	
	public ActionSubmitter(ProcessingAction action) {
		Preconditions.checkNotNull(action);
		this.action = action;
	}
	
	@Override
	public void run() {
	
		try {
			this.submitAction(this.action);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Submits an action to hadoop to be processed.
	 * @param action
	 */
	private void submitAction(ProcessingAction action) {
		
		//1. Check that the output of the action does not
		//exist.
		
		//1.1 If it exists, update action as finished in the
		//database
		
		//1.2 If it does not exist, update database with submitted
		//1.2.1 If database accepts update by comparing versions
		// Submit to Oozie.
		//1.2.2 If there is an error submitting action, update
		//database with state: Ready.
	}

}
