package io.biblia.workflows.manager.decision;

import java.util.List;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.google.common.base.Preconditions;

import java.util.Date;
import java.io.IOException;

import io.biblia.workflows.hdfs.HdfsUtil;
import io.biblia.workflows.manager.action.PersistedAction;
import io.biblia.workflows.Configuration;
import io.biblia.workflows.ConfigurationKeys;
import io.biblia.workflows.manager.dataset.DatasetPersistance;
import io.biblia.workflows.manager.dataset.DatasetState;
import io.biblia.workflows.manager.dataset.PersistedDataset;

public class DecisionManager {

	private static DecisionManager instance = null;
	
	private static Thread t;
	
	private ActionRollingWindow actionRollingWindow;
	
	private DatasetPersistance dPersistance;
	
	private static final Logger logger = Logger.getLogger(DecisionManager.class.getName());
	
	private DecisionAlgorithm decisionAlgorithm = new MostCommonlyUsedDecisionAlgorithm();
	
	private DecisionManager(DatasetPersistance dPersistance,
			DecisionAlgorithm decisionAlgorithm,
			ActionRollingWindow actionRollingWindow) {
		Preconditions.checkNotNull(dPersistance);
		Preconditions.checkNotNull(actionRollingWindow);
		Preconditions.checkNotNull(decisionAlgorithm);
		this.dPersistance = dPersistance;
		this.decisionAlgorithm = decisionAlgorithm;
		this.actionRollingWindow = actionRollingWindow;
		
		t = new Thread(new DecisionManagerRunner(), "DecisionManager thread");
		t.start();
	}
	
	private class DecisionManagerRunner implements Runnable, ConfigurationKeys {
		
		@Override
		public void run() {
			logger.info("Started DecisionManager");
			while (!Thread.currentThread().isInterrupted()) {
				try {
					//1. Get the dataset space used. If dataset space
					//is within a threshold percentage of total allowed
					//space, keep going:
					try {
						Long capacity = HdfsUtil.getFileSystemCapacityInMB();
						logger.log(Level.FINER, "Current system capacity in MB: {0}", capacity);
						Long used = HdfsUtil.getFileSystemUsedSpaceInMB();
						logger.log(Level.FINER, "Current system usagae in MB: {0}", used);
						float usageRatio = Float.parseFloat(Configuration.getValue(DECISIONMANAGER_USAGERATIO, "0.8"));
						if (used > capacity * usageRatio) {
							long spaceToDelete = (long)(used - capacity * usageRatio); 
							
							//2. Get the last actions
							List<PersistedAction> lastActions = actionRollingWindow.getLastActions(200);
							logger.log(Level.FINER, "Obtained the last {0} actions from the rolling window", lastActions.size());
							
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
							List<PersistedDataset> allDatasets = dPersistance.getAllStoredDatasets();
							
							logger.log(Level.FINER, "The number of datasets stored in the system is {0}", allDatasets.size());
							List<String> toDelete = decisionAlgorithm.toDelete(sWorkflow, allDatasets, spaceToDelete);
							
							//6. Mark those datasets STORED_TO_DELETE.
							for (String actionOutput : toDelete) {
								try {
									PersistedDataset dataset = dPersistance.getDatasetByPath(actionOutput);
									if (null != dataset) {
										dPersistance.updateDatasetState(dataset, DatasetState.STORED_TO_DELETE);
										logger.log(Level.INFO, "The state of dataset {0} was changed to STORED_TO_DELETE", dataset.getPath());
									}
								}
								catch(Exception e) {
									logger.severe("Unknown exception thrown when updating datasets state: " + e.toString());
									continue;
								}
								
								
							}
						}
					}
					catch (IOException ex) {
						logger.severe("IOException thrown when trying to get Hdfs usage info");
					}
					
					Thread.sleep(30000);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}
	
	public static void stop() {
		logger.info("Shutting down DecisionManager... ");
		if (null != t) {
			t.interrupt();
		}
		
	}
	
	public static void join() {
		try {
			t.join();
		}
		catch(InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}
	
	public static void start(DatasetPersistance persistance,
			DecisionAlgorithm decisionAlgorithm,
			ActionRollingWindow actionRollingWindow
			) {
		if (null == instance) {
			instance = new DecisionManager(persistance, 
					decisionAlgorithm, 
					actionRollingWindow);
		}
	}
	
}
