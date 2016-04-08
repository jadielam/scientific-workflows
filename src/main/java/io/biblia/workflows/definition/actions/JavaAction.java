package io.biblia.workflows.definition.actions;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import io.biblia.workflows.definition.Action;

/**
 * Defines a command line action.
 * A command line action takes input parameters, 
 * output parameters and configuration parameters.
 * 
 * To the command line actions all this parameters are 
 * passed as command line parameters, in the following order:
 * 1. First input parameters, in the order defined by the list.
 * 2. Then output parameters, in the order defined by the list.
 * 3. Then configuration parameters, in the order defined by the list.
 * @author jadiel
 *
 */
public class JavaAction extends Action {

	private final String mainClassName;
	private final String jobTracker;
	private final String nameNode;
	private final Set<String> parentActionNames;
	private final Map<String, String> inputParameters;
	private final Map<String, String> outputParameters;
	private final Map<String, String> configurationParameters;
	
	public JavaAction(String name, 
			String actionFolder,
			boolean forceComputation, 
			String mainClassName,
			String jobTracker,
			String nameNode,
			Set<String> parentActionNames,
			LinkedHashMap<String, String> inputParameters, 
			LinkedHashMap<String, String> outputParameters, 
			LinkedHashMap<String, String> configurationParameters) {
	
		super(name, actionFolder, forceComputation);
		Preconditions.checkNotNull(mainClassName);
		Preconditions.checkNotNull(jobTracker);
		Preconditions.checkNotNull(nameNode);
		Preconditions.checkNotNull(parentActionNames);
		Preconditions.checkNotNull(inputParameters);
		Preconditions.checkNotNull(outputParameters);
		Preconditions.checkNotNull(configurationParameters);
		this.mainClassName = mainClassName;
		this.jobTracker = jobTracker;
		this.nameNode = nameNode;
		this.parentActionNames = parentActionNames;
		this.inputParameters = Collections.unmodifiableMap(inputParameters);
		this.outputParameters = Collections.unmodifiableMap(outputParameters);
		this.configurationParameters = Collections.unmodifiableMap(configurationParameters);
	}

	public String getMainClassName() {
		return mainClassName;
	}
	
	public Set<String> getParentActionNames() {
		return parentActionNames;
	}

	public Map<String, String> getInputParameters() {
		return inputParameters;
	}

	public Map<String, String> getOutputParameters() {
		return outputParameters;
	}

	public Map<String, String> getConfigurationParameters() {
		return configurationParameters;
	}
}
