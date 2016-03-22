package io.biblia.workflows.definition;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

public class Workflow {

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
	
	private final Map<String, Set<String>> inputParametersIndex = new HashMap<String, Set<String>>();
	
	/**
	 * Map from outputParameter value to name of action that outputs it.
	 */
	private final Map<String, String> outputParametersIndex = new HashMap<String, String>();
	
	private final Map<String, Set<String>> augmentedParents = new HashMap<String, Set<String>>();
	
	public Workflow(String startAction, String endAction, List<Action> actions) throws InvalidWorkflowException {
		for (Action action : actions) {
			if (null != action) {
				String name = action.getName();
				this.actions.put(name, action);
				
				//1. Building the input parameters index.
				List<String> inputParameters = action.getInputParameters();
				for (String input : inputParameters) {
					if (this.inputParametersIndex.containsKey(input)) {
						this.inputParametersIndex.get(input).add(name);
					}
					else {
						Set<String> actionNames = new HashSet<String>();
						actionNames.add(name);
						this.inputParametersIndex.put(input, actionNames);
					}
				}
				
				//2. Building the output parameters index.
				List<String> outputParameters = action.getOutputParameters();
				for (String output : outputParameters) {
					if (this.outputParametersIndex.containsKey(output)) {
						throw new InvalidWorkflowException("The output parameter: " + output + " appears"
								+ "in more than one action");
					}
					else {
						this.outputParametersIndex.put(output, name);
					}
				}
				
				//3. Initializing the augmented parents map
				this.augmentedParents.put(name, new HashSet<String>());
				
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
	
	private enum Color {
		WHITE, GRAY, BLACK
	}
	
	/**
	 * 0. It expands the list of parentActions to not only be the ones defined by 
	 * the parentActionNames of each action, but also by the data dependencies among
	 * the actions.
	 *  
	 * 1. Determines if the workflow definition constitutes a valid Directed Acyclic Graph.
	 * In order to do that, it checks that there are no cycles in the graph.
	 * 
	 * 2. It also checks that the startActionName is not a descendant of any other
	 * action in the workflow.
	 * @throws InvalidWorkflowException if any of the constraints specified above are not
	 * met.
	 */
	private void validateWorkflow() throws InvalidWorkflowException {
		//0. Expand the set of parent nodes of each action
		//Challenge: I had made that set to be unmodifiable at 
		//creation time, so I will have to add an external datastructure 
		//to be able to achieve this.
		Set<Entry<String, Action>> entrySet = this.actions.entrySet();
		for (Entry<String, Action> e : entrySet) {
			String actionName = e.getKey();
			Action action = e.getValue();
			Set<String> parentNames = action.getParentActionNames();
			List<String> inputParameters = action.getInputParameters();
			for (String inputParameter : inputParameters) {
				String parentActionName = this.outputParametersIndex.get(inputParameter);
				if (null != parentActionName) {
					if (!parentNames.contains(parentActionName)) {
						this.augmentedParents.get(actionName).add(parentActionName);
					}
				}
			}
		}
		
		//1. Validate that there are no cycles in the graph.
		Map<String, Color> visited = new HashMap<String, Color>();
		Queue<String> actionsQueue = new ArrayDeque<String>();
		
		
		for (Entry<String, Action> e : entrySet) {
			String actionName = e.getKey();
			visited.put(actionName, Color.WHITE);
		}
		
		for (Entry<String, Action> e : entrySet) {
			String action = e.getKey();
			dfs(action, actionsQueue, visited);
		}
		
		//2. Validate that the start action is not a descendant of any other action.
		Action startAction = this.actions.get(this.startActionName);
		Collection<String> parentActions = startAction.getParentActionNames();
		if (null != parentActions && parentActions.size() > 0) {
			throw new InvalidWorkflowException("startAction has parent actions");
		}
	}
	
	private void dfs(String currentAction, Queue<String> actionsQueue, Map<String, Color> visited) throws InvalidWorkflowException {
		//1. If we have not visited this node yet, start visiting it
		if (visited.get(currentAction).equals(Color.WHITE)) {
			visited.put(currentAction, Color.GRAY);
			Action action = this.actions.get(currentAction);
			Set<String> parentActions = action.getParentActionNames();
			Set<String> augmentedParentActions = this.augmentedParents.get(currentAction);
			actionsQueue.addAll(parentActions);
			actionsQueue.addAll(augmentedParentActions);
			while (!actionsQueue.isEmpty()) {
				String nextAction = actionsQueue.poll();
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

	public String getStartAction() {
		return startActionName;
	}

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
}
