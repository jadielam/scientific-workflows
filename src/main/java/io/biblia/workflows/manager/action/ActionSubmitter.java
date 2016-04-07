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
		
		
		//1.2, Update database with submitted
		//1.2.1 If database accepts update by comparing versions
		// Submit to Oozie.
		//1.2.2 If there is an error submitting action, update
		//database with state: Ready.
		///1.2.3 Otherwise, if the submission of the action goes well
		//and I get back a workflow id, save the workflow id to the 
		//mongodatabase.  
		//
		//If there is an error talking to the database at any of this 
		//points, this is a critical error of the system and needs to 
		//be logged and taken care of.
		//
		
     
	}

}
