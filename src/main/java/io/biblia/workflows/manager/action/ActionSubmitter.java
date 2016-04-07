package io.biblia.workflows.manager.action;

import com.google.common.base.Preconditions;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.manager.oozie.OozieClientUtil;

public class ActionSubmitter implements Runnable {
	
	private final Action action;
	private final ActionPersistance persistance;
  
	public ActionSubmitter(Action action, ActionPersistance persistance) {
		Preconditions.checkNotNull(action);
      Preconditions.checkNotNull(persistance);
		this.action = action;
      this.persistance = persistance;
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
		
		
		  //1.2, Update database with submitted
		  try {
            this.persistance.updateActionState(action, ActionState.SUBMITTED);
        }
        catch (OutdatedActionException ex) {
            return;
        }
		
        //1.2.1 If database accepts update by comparing versions
		  // Submit to Oozie.
		  try{
			   String submissionId = OozieClientUtil.submitAndStartOozieJob(action);
        }
        catch(OozieClientException | IOException ex) {
            ex.printStackTrace();
            
             //	1.2.1.1 If there is an error submitting action, update
				//database with state: READY.
				try{
            	 this.persistance.updateActionState(action, ActionState.READY);  
            }
            catch(OutdatedActionException ex) {
                return;
            }
				
            return;
        }
        try {                        
            this.persistance.addActionSubmissionId(action, submissionId);
        }
        catch(OutdatedActionException ex) {
            //This exception is not supposed to be thrown in here. Log the error
            //as a bug to be fixed later on.
        }
     
	}

}
