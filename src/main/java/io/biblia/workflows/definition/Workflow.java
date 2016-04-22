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
	private final String startActionName;
	
	/**
	 * Name of the end action of the workflow.
	 */
	private final String endActionName;
	
	/**
	 * Contains all the actions of the workflow, keyed by the action name.
	 */
	private final Map<String, Action> actions = new HashMap<String, Action>();
	
	private final Set<Action> actionsSet;
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
	public Workflow(String workflowName, String startAction, String endAction, Set<Action> actions) throws InvalidWorkflowException {
		Preconditions.checkNotNull(workflowName);
		Preconditions.checkNotNull(startAction);
		Preconditions.checkNotNull(endAction);
		Preconditions.checkNotNull(actions);
		this.actionsSet = actions;
		this.workflowName = workflowName;
		for (Action action : actions) {
			if (null != action) {
				String name = action.getUniqueName();
				this.actions.put(name, action);
				for (Action parentAction : action.getParents()) {
					if (!this.actionsDependency.containsKey(parentAction)) {
						this.actionsDependency.put(parentAction, new HashSet<Action>());
					}
					this.actionsDependency.get(parentAction).add(action);
				}				
			}
		}
		if (this.actions.containsKey(startAction)) {
			this.startActionName = startAction;
		} 
		else {
			throw new InvalidWorkflowException("Start action: " + startAction + " is not one of the actions of the workflow");
		}
		if (this.actions.containsKey(endAction)) {
			this.endActionName = endAction;
		}
		else {
			throw new InvalidWorkflowException("End action: " + endAction + " is not one of the actions of the workflow");
		}
		validateWorkflow();
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
		Set<Entry<String, Action>> entrySet = this.actions.entrySet();
		
		//1. Validate that there are no cycles in the graph.
		Map<Action, Color> visited = new HashMap<Action, Color>();
		Queue<Action> actionsQueue = new ArrayDeque<Action>();
		
		for (Entry<String, Action> e : entrySet) {
			Action actionName = e.getValue();
			visited.put(actionName, Color.WHITE);
		}
		
		for (Entry<String, Action> e : entrySet) {
			Action action = e.getValue();
			dfs(action, actionsQueue, visited);
		}
		
		//2. Validate that the start action is not a descendant of any other action.
		Action startAction = this.actions.get(this.startActionName);
		Collection<Action> parentActions = startAction.getParents();
		if (null != parentActions && parentActions.size() > 0) {
			throw new InvalidWorkflowException("startAction has parent actions");
		}
		
		//3. Validate that all the referenced actions are defined.
		for (Entry<String, Action> e : entrySet) {
			Action action = e.getValue();
			Collection<Action> parentNames = action.getParents();
			for (Action parent : parentNames) {
				if (!this.actionsSet.contains(parent)) {
					throw new InvalidWorkflowException("Action: " + parent.getOriginalName() + " is referenced but not"
							+ "defined in the workflow.");
				}
			}
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
	private void dfs(Action currentAction, Queue<Action> actionsQueue, Map<Action, Color> visited) throws InvalidWorkflowException {
		//1. If we have not visited this node yet, start visiting it
		if (visited.get(currentAction).equals(Color.WHITE)) {
			visited.put(currentAction, Color.GRAY);
			Action action = this.actions.get(currentAction);
			List<Action> parentActions = action.getParents();
			actionsQueue.addAll(parentActions);
			while (!actionsQueue.isEmpty()) {
				Action nextAction = actionsQueue.poll();
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
	public String getStartAction() {
		return startActionName;
	}

	/**
	 * Returns the name of the end action
	 * @return
	 */
	public String getEndAction() {
		return endActionName;
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
