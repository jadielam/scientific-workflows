package io.biblia.workflows.definition.actions;

import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

import io.biblia.workflows.definition.Action;

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
