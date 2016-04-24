package io.biblia.workflows.definition;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
	 * Contains all the actions of the workflow, keyed by the action name.
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
		validateWorkflow();
		
		//3. Set long name of all actions.
		//TODO: Make topological sort first.
		for (Action action : actions) {
			//TODO: make check that action is of the type that takes natural order.
			//TODO: Maybe I need the action inside handle this, all I do is pass the long
			//names of its parents and then I let the action figure out what to do with them
			
			//ActionUtils.createActionLongNameNaturalOrder(uniqueName, parents)
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
	 * @throws InvalidWorkflowException if any of the constraints specified above are not
	 * met.
	 */
	private void validateWorkflow() throws InvalidWorkflowException {
		Set<Entry<Integer, Action>> entrySet = this.actions.entrySet();
		
		//1. Validate that there are no cycles in the graph.
		Map<Action, Color> visited = new HashMap<Action, Color>();
		Queue<Integer> actionsQueue = new ArrayDeque<>();
		
		for (Entry<Integer, Action> e : entrySet) {
			Action actionName = e.getValue();
			visited.put(actionName, Color.WHITE);
		}
		
		for (Entry<Integer, Action> e : entrySet) {
			Action action = e.getValue();
			dfs(action, actionsQueue, visited);
		}
		
		//2. Validate that the start action is not a descendant of any other action.
		Action startAction = this.actions.get(this.startActionId);
		Collection<Integer> parentActions = startAction.getParentIds();
		if (null != parentActions && parentActions.size() > 0) {
			throw new InvalidWorkflowException("startAction has parent actions");
		}
		
	}
	
	/**
	 * This is a modification of the dfs algorithm that is used to detect if there
	 * are cycles in the graph.
	 * @param currentAction The action being explored
	 * @param actionsQueue Datastructure to keep the next actions to be explored.
	 * @param visited a Map that indicates if a node has been explored or not.
	 * Color.WHITE means not explored
	 * Color.GRAY means exploring
	 * Color.BLACK means explored
	 * @throws InvalidWorkflowException
	 */
	private void dfs(Action currentAction, Queue<Integer> actionsQueue, Map<Action, Color> visited) throws InvalidWorkflowException {
		//1. If we have not visited this node yet, start visiting it
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
		}
		//2. Else if this node is currently being visited,
		//this is a cycle and we are in trouble, throw an exception	
		else if (visited.get(currentAction).equals(Color.GRAY)) {
			throw new InvalidWorkflowException("Cycle found going to: " + currentAction);
		}
		//3. If the node is black, do nothing. It has been opened and closed, and there is no need to
		//visit it again.
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
	public Action getAction(String name) {
		return this.actions.get(name);
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
}
