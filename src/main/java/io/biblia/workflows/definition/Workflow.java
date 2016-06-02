package io.biblia.workflows.definition;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.LinkedHashMap;

import com.google.common.base.Preconditions;

import java.util.Queue;
import java.util.Set;

/**
 * Class that defines a workflow.
 * A workflow is defined by its start action,
 * its end action, and a directed acyclic graph
 * of action dependencies.
 * @author jadiel
 */
public class Workflow {

	/**
	 * Name of the workflow.
	 */
	private final String workflowName;
	
	/**
	 * Name of the starting action of the workflow.
	 */
	private final int startActionId;
	
	/**
	 * Name of the end action of the workflow.
	 */
	private final int endActionId;
	
	/**
	 * Contains all the actions of the workflow, keyed by the action id.
	 */
	private final Map<Integer, Action> actions = new HashMap<>();
	
	/**
	 * Map from action to actions that depend on it.
	 */
	private final Map<Action, Set<Action>> actionsDependency = new HashMap<>();
	
	/**
	 * Constructs a workflow object.  It first validates the workflow
	 * @param startAction
	 * @param endAction
	 * @param actions
	 * @throws InvalidWorkflowException if the workflow is not valid: if the workflow has a cycle or 
	 * an action is referenced but not defined, or if an output parameter appears in more than one
	 * action.
	 * @throws NullPointerException if any of the parameters passed is null.
	 */
	public Workflow(String workflowName, int startActionId, int endActionId, List<Action> actions) throws InvalidWorkflowException {
		Preconditions.checkNotNull(workflowName);
		Preconditions.checkNotNull(actions);
		this.workflowName = workflowName;
		
		//1. Map from id to action
		for (Action action : actions) {
			if (null != action) {
				int id = action.getActionId();
				if (actions.contains(id)) {
					throw new InvalidWorkflowException("There is more than one action with the same id: "+ id);
				}
				this.actions.put(id, action);				
			}
		}
		
		//2. Dependency map
		for (Action action : actions) {
			for (int parentActionId : action.getParentIds()) {
				
				Action parentAction = this.actions.get(parentActionId);
				if (null == parentAction) {
					throw new InvalidWorkflowException("The action with id " + parentActionId + " is "
							+ "not defined");
				}
				
				if (!this.actionsDependency.containsKey(parentAction)) {
					this.actionsDependency.put(parentAction, new HashSet<Action>());
				}
				this.actionsDependency.get(parentAction).add(action);
			}
		}
		
		if (this.actions.containsKey(startActionId)) {
			this.startActionId = startActionId;
		} 
		else {
			throw new InvalidWorkflowException("Start action: " + startActionId + " is not one of the actions of the workflow");
		}
		if (this.actions.containsKey(endActionId)) {
			this.endActionId = endActionId;
		}
		else {
			throw new InvalidWorkflowException("End action: " + endActionId + " is not one of the actions of the workflow");
		}
		
		List<List<Action>> topologicalSorts = validateWorkflow();
		
		//3. Set long name of all actions.
		for (List<Action> subgraph : topologicalSorts) {
			for (Action action : subgraph) {
				
				List<Integer> parentsIds = action.getParentIds();
				List<Action> parents = new ArrayList<>();
				List<List<String>> parentsLongNames = new ArrayList<>();
				LinkedHashMap<String, String> parentsOutput = new LinkedHashMap<>();
				for (Integer id : parentsIds) {
					if (this.actions.containsKey(id)) {
						Action parent = this.actions.get(id);
						List<String> longName = parent.getLongName();
						String output = parent.getOutputPath();
						parents.add(parent);
						parentsLongNames.add(longName);
						parentsOutput.put(id.toString(), output);
					}
				}
				
				//1. set the long name of the action.
				action.setLongName(parentsLongNames);
				
				//2. set the input parameters of the action
				action.setInputParameters(parentsOutput); 		
			}
		}
		
		
	}
	
	/**
	 * Enum used by the DFS algorithm
	 * 
	 * Color.WHITE means not explored
	 * Color.GRAY means exploring
	 * Color.BLACK means explored
	 * @author jadiel
	 *
	 */
	private enum Color {
		WHITE, GRAY, BLACK
	}
	
	/**
	 * 1. Determines if the workflow definition constitutes a valid Directed Acyclic Graph.
	 * In order to do that, it checks that there are no cycles in the graph.
	 * 
	 * 2. It also checks that the startActionName is not a descendant of any other
	 * action in the workflow.
	 * 
	 * TODO:
	 * 3. If an action is forceComputation, it will make all its descendants also forceComputation
	 * 4. If an action is manageYourself, it will make all its descendants that are not manageYourself
	 * to be forceComputation.
	 * 
	 * @throws InvalidWorkflowException if any of the constraints specified above are not
	 * met.
	 * 
	 * @return Returns a list of lists topologically sorted.  More than one list indicates
	 * that the graph had more than one independent subgraphs.
	 */
	private List<List<Action>> validateWorkflow() throws InvalidWorkflowException {
		Set<Entry<Integer, Action>> entrySet = this.actions.entrySet();
		List<List<Action>> topologicalSorts = new ArrayList<>();
		//1. Validate that there are no cycles in the graph.
		Map<Action, Color> visited = new HashMap<Action, Color>();
		Queue<Integer> actionsQueue = new ArrayDeque<>();
		
		for (Entry<Integer, Action> e : entrySet) {
			Action actionName = e.getValue();
			visited.put(actionName, Color.WHITE);
		}
		
		for (Entry<Integer, Action> e : entrySet) {
			Action action = e.getValue();
			List<Action> topologicalSort = dfs(action, actionsQueue, visited);
			if (topologicalSort.size() > 0) {
				topologicalSorts.add(topologicalSort);
			}
		}
		
		//2. Validate that the start action is not a descendant of any other action.
		Action startAction = this.actions.get(this.startActionId);
		Collection<Integer> parentActions = startAction.getParentIds();
		if (null != parentActions && parentActions.size() > 0) {
			throw new InvalidWorkflowException("startAction has parent actions");
		}
		
		return topologicalSorts;
	}
	
	/**
	 * This is a modification of the dfs algorithm that is used to detect if there
	 * are cycles in the graph.  It also returns a list of actions topologically sorted
	 * @param currentAction The action being explored
	 * @param actionsQueue Datastructure to keep the next actions to be explored.
	 * @param visited a Map that indicates if a node has been explored or not.
	 * Color.WHITE means not explored
	 * Color.GRAY means exploring
	 * Color.BLACK means explored
	 * @return Topologically sorted list
	 * @throws InvalidWorkflowException
	 */
	private List<Action> dfs(Action currentAction, Queue<Integer> actionsQueue, Map<Action, Color> visited) throws InvalidWorkflowException {
		//1. If we have not visited this node yet, start visiting it
		LinkedList<Action> topologicalSort = new LinkedList<>();
		if (visited.get(currentAction).equals(Color.WHITE)) {
			visited.put(currentAction, Color.GRAY);
			Action action = this.actions.get(currentAction);
			List<Integer> parentActionsIds = action.getParentIds();
			actionsQueue.addAll(parentActionsIds);
			while (!actionsQueue.isEmpty()) {
				Integer nextActionId = actionsQueue.poll();
				Action nextAction = this.actions.get(nextActionId);
				dfs(nextAction, actionsQueue, visited);
			}
			visited.put(currentAction, Color.BLACK);
			topologicalSort.addFirst(currentAction);
		}
		//2. Else if this node is currently being visited,
		//this is a cycle and we are in trouble, throw an exception	
		else if (visited.get(currentAction).equals(Color.GRAY)) {
			throw new InvalidWorkflowException("Cycle found going to: " + currentAction);
		}
		//3. If the node is black, do nothing. It has been opened and closed, and there is no need to
		//visit it again.
		
		return topologicalSort;
	}

	/**
	 * Returns the name of the start action
	 * @return
	 */
	public int getStartActionId() {
		return startActionId;
	}

	/**
	 * Returns the name of the end action
	 * @return
	 */
	public int getEndActionId() {
		return endActionId;
	}
	
	/**
	 * Returns an action object given a name.
	 * Returns null if no action of that name 
	 * is found.
	 * @param name
	 * @return
	 */
	public Action getAction(Integer id) {
		return this.actions.get(id);
	}
	
	public Collection<Action> getActions() {
		return Collections.unmodifiableCollection(this.actions.values());
	}
	/**
	 * Returns the name of the workflow.
	 * @return
	 */
	public String getWorkflowName() {
		return this.workflowName;
	}
	
	/**
	 * Returns the child actions for a given actionId.
	 * Returns null if the action id is invalid.
	 * @param actionId
	 * @return
	 */
	public Collection<Action> getChildActions(Integer actionId) {
		Action action = this.actions.get(actionId);
		if (null != action) {
			return this.actionsDependency.get(action);
		}
		else {
			return null;
		}
	}
}
