package io.biblia.workflows.manager.decision;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Date;

/**
 * The simplified workflow keeps data of the workflow that
 * will be enough for the algorithm to take decisions.
 * 
 * @author dearj019
 *
 */
public class SimplifiedWorkflow {

	/**
	 * Contains an adjacency list of the graph of action
	 * dependencies, where actions are identified
	 * by their output path.
	 */
	private final Map<String, List<String>> adjacencyList;
	
	/**
	 * Map from an action id (its output path) to the rest 
	 * of data that is needed from that action
	 * by the decision algorithm. 
	 */
	private final Map<String, ActionData> actionsData;
	
	public SimplifiedWorkflow() {
		this.adjacencyList = new HashMap<>();
		this.actionsData = new HashMap<>();
	}
	
	/**
	 * 
	 * @param outputData the output dataset path of this action
	 * @param parentsOutputData the output dataset path of its parent actions
	 * @param workflowId The id of the workflow to which the action belonged when it was submitted.
 	 * @param sizeInMB The size of the output data of the action in MB
	 * @param startTime The time when the action started being executed
	 * @param endTime The time when the action finished being executed.
	 */
	public void addAction(String outputData, List<String> parentsOutputData,
			Long workflowId, Double sizeInMB, Date startTime, Date endTime) {
		
		if (!this.adjacencyList.containsKey(outputData)) {
			//this.adjacencyList.put(outputData, **value);
		}
	}
 }

class Tuple {
	
	private final String outputDataset;
	
	private final Long workflowId;
	
	public Tuple(String outputDataset, Long workflowId) {
		this.outputDataset = outputDataset;
		this.workflowId = workflowId;
	}

	public String getOutputDataset() {
		return outputDataset;
	}

	public Long getWorkflowId() {
		return workflowId;
	}
	
}
class ActionData {
	
	/**
	 * Size of the output dataset of the action
	 */
	private final double sizeInMB;
	
	/**
	 * Time when computation started
	 */
	private final Date startTime;
	
	/**
	 * Time when computation ended
	 */
	private final Date endTime;
	
	
	public ActionData(double sizeInMB, Date startTime,
			Date endTime) {
		this.sizeInMB = sizeInMB;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public double getSizeInMB() {
		return sizeInMB;
	}

	public Date getStartTime() {
		return startTime;
	}

	public Date getEndTime() {
		return endTime;
	}
	
}
