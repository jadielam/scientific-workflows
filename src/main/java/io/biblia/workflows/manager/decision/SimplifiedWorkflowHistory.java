package io.biblia.workflows.manager.decision;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Collection;
import java.util.Date;

/**
 * The simplified workflow keeps data of the workflow that
 * will be enough for the algorithm to take decisions.
 * 
 * @author dearj019
 *
 */
public class SimplifiedWorkflowHistory {

	/**
	 * Contains an adjacency list of the graph of action
	 * dependencies, where actions are identified
	 * by their output path.
	 */
	private final Map<Tuple, List<String>> adjacencyList;
	
	/**
	 * Map from an action to a list of the workflows where this
	 * action was executed.
	 */
	private final Map<String, List<Long>> actionsWorkflows;
	
	/**
	 * Contains the number of times an action happened.
	 */
	private final Map<String, Integer> actionsCount;
	
	/**
	 * Map from an action output path to the rest 
	 * of data that is needed from that action
	 * by the decision algorithm. 
	 */
	private final Map<String, ActionData> actionsData;
	
	public SimplifiedWorkflowHistory() {
		this.adjacencyList = new HashMap<>();
		this.actionsData = new HashMap<>();
		this.actionsCount = new HashMap<>();
		this.actionsWorkflows = new HashMap<>();
	}
	
	/**
	 * If the outputData has already been inserted, it keeps the last insertion
	 * data.  Less simpler implementations may add other logic such as simply using
	 * the average of a list of entries.
	 * @param outputData the output dataset path of this action
	 * @param parentsOutputData the output dataset path of its parent actions
	 * @param workflowId The id of the workflow to which the action belonged when it was submitted.
 	 * @param sizeInMB The size of the output data of the action in MB
	 * @param startTime The time when the action started being executed
	 * @param endTime The time when the action finished being executed.
	 */
	public void addAction(String outputPath, List<String> parentsOutputData,
			Long workflowId, Double sizeInMB, Date startTime, Date endTime) {
		
		//Updating actionsData
		ActionData actionData = new ActionData(sizeInMB, startTime, endTime,
				workflowId);
		this.actionsData.put(outputPath, actionData);
		
		//Updating actionsWorkflows
		if (!this.actionsWorkflows.containsKey(outputPath)) {
			List<Long> workflows = new LinkedList<>();
			workflows.add(workflowId);
			this.actionsWorkflows.put(outputPath, workflows);
		}
		else {
			this.actionsWorkflows.get(outputPath).add(workflowId);
		}
		
		//Updating adjacencyList
		Tuple tuple = new Tuple(workflowId, outputPath);
		this.adjacencyList.put(tuple, parentsOutputData);
		
		//Updating actionsCount
		if (!this.actionsCount.containsKey(outputPath)) {
			this.actionsCount.put(outputPath, Integer.valueOf(1));
		}
		else {
			this.actionsCount.put(outputPath, this.actionsCount.get(outputPath) + 1);
		}
	}
	
	public Set<String> getActions() {
		return this.actionsData.keySet();
	}
	
	public List<String> getActionChildren(String actionOutputPath) {
		return this.adjacencyList.get(actionOutputPath);
	}
	
	public Integer getActionCount(String actionOutputPath) {
		return this.actionsCount.get(actionOutputPath);
	}
	
	public ActionData getActionData(String actionOutputPath) {
		return this.actionsData.get(actionOutputPath);
	}
	
	public List<Long> getActionWorkflows(String actionOutputPath) {
		return this.actionsWorkflows.get(actionOutputPath);
	}
 }

class Tuple {
	private final Long workflowId;
	
	private final String outputPath;
	
	public Tuple(Long workflowId, String outputPath) {
		this.workflowId = workflowId;
		this.outputPath = outputPath;
	}

	
	public Long getWorkflowId() {
		return workflowId;
	}


	public String getOutputPath() {
		return outputPath;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((outputPath == null) ? 0 : outputPath.hashCode());
		result = prime * result + ((workflowId == null) ? 0 : workflowId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Tuple other = (Tuple) obj;
		if (outputPath == null) {
			if (other.outputPath != null)
				return false;
		} else if (!outputPath.equals(other.outputPath))
			return false;
		if (workflowId == null) {
			if (other.workflowId != null)
				return false;
		} else if (!workflowId.equals(other.workflowId))
			return false;
		return true;
	}
	
	
}
class ActionData {
	
	/**
	 * The id of the workflow to which this action belongs to.
	 */
	private final Long workflowId;
	
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
			Date endTime, Long workflowId) {
		this.sizeInMB = sizeInMB;
		this.startTime = startTime;
		this.endTime = endTime;
		this.workflowId = workflowId;
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
	
	public Long getWorkflowId() {
		return workflowId;
	}
}

