package io.biblia.workflows.manager.decision;

import java.util.List;

import io.biblia.workflows.manager.action.PersistedAction;

public class DecisionManager {

	private static DecisionManager instance = null;
	
	private static Thread t;
	
	private ActionRollingWindow actionRollingWindow;
	
	private DatasetLogDao datasetLogDao;
	
	private class DecisionManagerRunner implements Runnable {
		
		@Override
		public void run() {
			while (!Thread.currentThread().isInterrupted()) {
				try {
					//1. Get the dataset space used. If dataset space
					//is within a threshold percentage of total allowed
					//space, keep going:
					
					//2. Get the last actions
					List<PersistedAction> lastActions = actionRollingWindow.getLastActions(200);
					
					//3. Create a SimplifiedWorkflow
					
					//4. Pass to the algorithm the space to free
					//and the simplified workflow.
					
					//5. Get the list of datasets to mark TO_DELETE
					
					//6. Mark those datasets TO_DELETE.
					Thread.sleep(1000);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
	
}
