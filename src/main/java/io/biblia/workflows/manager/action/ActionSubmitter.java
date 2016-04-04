package io.biblia.workflows.manager.action;

import java.util.concurrent.BlockingQueue;

import com.google.common.base.Preconditions;

public class ActionSubmitter implements Runnable {

	private final BlockingQueue<ProcessingAction> actionsQueue;
	
	public ActionSubmitter(BlockingQueue<ProcessingAction> actionsQueue) {
		Preconditions.checkNotNull(actionsQueue);
		this.actionsQueue = actionsQueue;
	}
	@Override
	public void run() {
		
		while (true) {
			try {
				ProcessingAction action = this.actionsQueue.take();
				this.submitAction(action);
				
				try{
					Thread.sleep(100);
				}
				catch(InterruptedException ex) {
					continue;
				}
			}
			catch(Exception e) {
				e.printStackTrace();
			}
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
