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
	private final Map<String, ManagedAction> actions = new HashMap<String, ManagedAction>();
	
	/**
	 * Map from input parameter to action names that depend on it.
	 */
	private final Map<String, Set<String>> inputParametersIndex = new HashMap<String, Set<String>>();
	
	/**
	 * Map from outputParameter value to name of action that outputs it.
	 */
	private final Map<String, String> outputParametersIndex = new HashMap<String, String>();
	
	/**
	 * Map from action name to a set of augmented parents. 
	 * Augmented parents are implicit parents that are not defined
	 * as parents in the action definition, yet the action
	 * depends on them because they produce output that the 
	 * action uses as input.
	 */
	private final Map<String, Set<String>> augmentedParents = new HashMap<String, Set<String>>();
	
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
	public Workflow(String workflowName, String startAction, String endAction, List<ManagedAction> actions) throws InvalidWorkflowException {
		Preconditions.checkNotNull(workflowName);
		Preconditions.checkNotNull(startAction);
		Preconditions.checkNotNull(endAction);
		Preconditions.checkNotNull(actions);
		this.workflowName = workflowName;
		for (ManagedAction action : actions) {
			if (null != action) {
				String name = action.getName();
				this.actions.put(name, action);
				
				//1. Building the input parameters index.
				Map<String, String> inputParameters = action.getInputParameters();
				for (Entry<String, String> e : inputParameters.entrySet()) {
					String input = e.getValue();
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
				Map<String, String> outputParameters = action.getOutputParameters();
				for (Entry<String, String> e : outputParameters.entrySet()) {
					String output = e.getValue();
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
		Set<Entry<String, ManagedAction>> entrySet = this.actions.entrySet();
		for (Entry<String, ManagedAction> e : entrySet) {
			String actionName = e.getKey();
			ManagedAction action = e.getValue();
			Set<String> parentNames = action.getParentActionNames();
			Map<String, String> inputParameters = action.getInputParameters();
			for (Entry<String, String> e1 : inputParameters.entrySet()) {
				String inputParameter = e1.getValue();
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
		
		
		for (Entry<String, ManagedAction> e : entrySet) {
			String actionName = e.getKey();
			visited.put(actionName, Color.WHITE);
		}
		
		for (Entry<String, ManagedAction> e : entrySet) {
			String action = e.getKey();
			dfs(action, actionsQueue, visited);
		}
		
		//2. Validate that the start action is not a descendant of any other action.
		ManagedAction startAction = this.actions.get(this.startActionName);
		Collection<String> parentActions = startAction.getParentActionNames();
		if (null != parentActions && parentActions.size() > 0) {
			throw new InvalidWorkflowException("startAction has parent actions");
		}
		
		//3. Validate that all the referenced actions are defined.
		for (Entry<String, ManagedAction> e : entrySet) {
			ManagedAction action = e.getValue();
			Set<String> parentNames = action.getParentActionNames();
			for (String parentName : parentNames) {
				if (!this.actions.containsKey(parentName)) {
					throw new InvalidWorkflowException("Action: " + parentName + " is referenced but not"
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
	private void dfs(String currentAction, Queue<String> actionsQueue, Map<String, Color> visited) throws InvalidWorkflowException {
		//1. If we have not visited this node yet, start visiting it
		if (visited.get(currentAction).equals(Color.WHITE)) {
			visited.put(currentAction, Color.GRAY);
			ManagedAction action = this.actions.get(currentAction);
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
	public ManagedAction getAction(String name) {
		return this.actions.get(name);
	}
	
	public Collection<ManagedAction> getActions() {
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
