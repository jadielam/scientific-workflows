package io.biblia.workflows.definition.actions;

import java.util.List;

import io.biblia.workflows.definition.Action;

public class CommandLineAction implements Action {

	private final String name;
	private final String mainClassName;
	private final List<String> parentActionNames;
	private final List<String> inputParameters;
	private final List<String> outputParameters;
	private final List<String> configurationParameters;
	
	public CommandLineAction(String name, String mainClassName,
			List<String> parentActionNames,
			List<String> inputParameters, List<String> outputParameters, 
			List<String> configurationParameters) {
	
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
	
	public List<String> getParentActionNames() {
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
