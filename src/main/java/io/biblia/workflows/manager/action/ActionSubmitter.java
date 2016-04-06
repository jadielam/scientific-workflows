package io.biblia.workflows.manager.action;

import com.google.common.base.Preconditions;

import io.biblia.workflows.definition.Action;

public class ActionSubmitter implements Runnable {
	
	private final Action action;
	
	public ActionSubmitter(Action action) {
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
	private void submitAction(Action action) {
		
		//TODO
		//1. Check if I need to compute the action or not.
		//1.1 Check if the actions is forced to be computed
		//To check if the output of the action exists or not, I need to get
		//the output files of the action
		
		if (needToCompute(action)) {
			//1. Update database with submitted
			//2. If database accepts update, submit to Oozie
			//3. If there is an error submitting action, update database
			//with state Ready.
		} 
		else {
			//Update action as finished in the
			//database
			
		}
		
		
		//1.2 If it does not exist, update database with submitted
		//1.2.1 If database accepts update by comparing versions
		// Submit to Oozie.
		//1.2.2 If there is an error submitting action, update
		//database with state: Ready.
	}
	
	/**
	 * Returns true if the action needs to be computed, otherwise
	 * returns false.
	 * @param action
	 * @return
	 */
	private boolean needToCompute(Action action) {
		//TODO
		//1. CHeck if action is forced to be computed.
		if (action.getForceComputation()) {
			return true;
		}
		else {
			//2. If not, check if action output exists:
			//2.1 For each of the outputs of the action:
			//2.1.1 Check that the folder exists and that is not empty.
			//2.1.2 Check that the folder metadata values are identical
			//to the metadata values stored in the database.
			return true;
		}
	}

}
