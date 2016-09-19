package io.biblia.workflows.manager.decision;

import java.util.List;
import java.util.Date;
import java.io.IOException;


import io.biblia.workflows.hdfs.HdfsUtil;
import io.biblia.workflows.manager.action.PersistedAction;
import io.biblia.workflows.Configuration;
import io.biblia.workflows.ConfigurationKeys;
import io.biblia.workflows.hdfs.HdfsUtil;

public class DecisionManager {

	private static DecisionManager instance = null;
	
	private static Thread t;
	
	private ActionRollingWindow actionRollingWindow;
	
	private DatasetLogDao datasetLogDao;
	
	private DecisionAlgorithm decisionAlgorithm = new MostCommonlyUsedDecisionAlgorithm();
	
	private class DecisionManagerRunner implements Runnable, ConfigurationKeys {
		
		@Override
		public void run() {
			while (!Thread.currentThread().isInterrupted()) {
				try {
					//1. Get the dataset space used. If dataset space
					//is within a threshold percentage of total allowed
					//space, keep going:
					try {
						Long capacity = HdfsUtil.getFileSystemCapacityInMB();
						Long used = HdfsUtil.getFileSystemUsedSpaceInMB();
						float usageRatio = Float.parseFloat(Configuration.getValue(DECISIONMANAGER_USAGERATIO, "0.8"));
						if (used > capacity * usageRatio) {
							long spaceToDelete = (long)(used - capacity * usageRatio); 
							
							//2. Get the last actions
							List<PersistedAction> lastActions = actionRollingWindow.getLastActions(200);
							
							//3. Create a SimplifiedWorkflow
							SimplifiedWorkflowHistory sWorkflow = new SimplifiedWorkflowHistory();
							for (PersistedAction action : lastActions) {
								String outputPath = action.getAction().getOutputPath();
								Double sizeInMB = action.getSizeInMB();
								Date startTime = action.getStartTime();
								Date endTime = action.getEndTime();
								List<String> parentOutputPaths = action.getParentActionOutputs();
								Long workflowId = action.getWorkflowId();
								
								sWorkflow.addAction(outputPath, parentOutputPaths, workflowId, sizeInMB, startTime, endTime);
							}
							
							//4. Pass to the algorithm the space to free
							//and the simplified workflow.
							//5. Get the list of datasets to mark TO_DELETE
							List<String> toDelete = decisionAlgorithm.toDelete(sWorkflow, spaceToDelete);
							
							//6. Mark those datasets TO_DELETE.
						}
					}
					catch (IOException ex) {
						//TODO: Log error here.
					}
					
					Thread.sleep(1000);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
	
}
