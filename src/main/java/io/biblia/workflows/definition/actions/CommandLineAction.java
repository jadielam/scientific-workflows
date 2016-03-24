package io.biblia.workflows.definition.actions;

import java.util.List;
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
public class CommandLineAction implements Action {

	private final String name;
	private final String mainClassName;
	private final Set<String> parentActionNames;
	private final List<String> inputParameters;
	private final List<String> outputParameters;
	private final List<String> configurationParameters;
	
	public CommandLineAction(String name, String mainClassName,
			Set<String> parentActionNames,
			List<String> inputParameters, List<String> outputParameters, 
			List<String> configurationParameters) {
	
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(mainClassName);
		Preconditions.checkNotNull(parentActionNames);
		Preconditions.checkNotNull(inputParameters);
		Preconditions.checkNotNull(outputParameters);
		Preconditions.checkNotNull(configurationParameters);
		this.name = name;
		this.mainClassName = mainClassName;
		this.parentActionNames = parentActionNames;
		this.inputParameters = inputParameters;
		this.outputParameters = outputParameters;
		this.configurationParameters = configurationParameters;
	}

	public String getName() {
		return name;
	}

	public String getMainClassName() {
		return mainClassName;
	}
	
	public Set<String> getParentActionNames() {
		return parentActionNames;
	}

	public List<String> getInputParameters() {
		return inputParameters;
	}

	public List<String> getOutputParameters() {
		return outputParameters;
	}

	public List<String> getConfigurationParameters() {
		return configurationParameters;
	}
}
