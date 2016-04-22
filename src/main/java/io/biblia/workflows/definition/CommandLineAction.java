package io.biblia.workflows.definition;

import java.util.LinkedHashMap;
import java.util.List;

import org.bson.Document;

public class CommandLineAction extends Action {

	private final String uniqueName;
	
	private final List<String> longName;
	
	private final String outputPath;
	
	private final String mainClassName;
	
	private final String jobTracker;
	
	private final String nameNode;

	/**
	 * Constructor for managed action
	 * @param name
	 * @param actionFolder
	 * @param type
	 * @param additionalInput
	 * @param configuration
	 * @param parents
	 * @param forceComputation
	 * @throws InvalidWorkflowException
	 */
	public CommandLineAction(String name, String actionFolder,
			LinkedHashMap<String, String> additionalInput, 
			LinkedHashMap<String, String> configuration,
			List<Action> parents, boolean forceComputation,
			String mainClassName, String jobTracker,
			String nameNode
			) 
					throws InvalidWorkflowException {
		super(name, actionFolder, ActionType.COMMAND_LINE, additionalInput, 
				configuration, parents, forceComputation, true);
		this.uniqueName = ActionUtils.createActionUniqueNameNaturalOrder(name, additionalInput, configuration);
		this.longName = ActionUtils.createActionLongNameNaturalOrder(this.uniqueName, parents);
		this.outputPath = ActionUtils.generateOutputPathFromLongName(this.longName);
		this.mainClassName = mainClassName;
		this.jobTracker = jobTracker;
		this.nameNode = nameNode;
	}
	
	/**
	 * Constructor for unamanged action
	 * @param name
	 * @param actionFolder
	 * @param type
	 * @param additionalInput
	 * @param configuration
	 * @param parents
	 * @param forceComputation
	 * @param outputPath
	 * @throws InvalidWorkflowException
	 */
	public CommandLineAction(String name, String actionFolder,
			LinkedHashMap<String, String> additionalInput, 
			LinkedHashMap<String, String> configuration,
			List<Action> parents, boolean forceComputation,
			String outputPath,
			String mainClassName, String jobTracker,
			String nameNode
			) 
					throws InvalidWorkflowException {
		super(name, actionFolder, ActionType.COMMAND_LINE, additionalInput, 
				configuration, parents, forceComputation, false);
		this.uniqueName = ActionUtils.createActionUniqueNameNaturalOrder(name, additionalInput, configuration);
		this.longName = ActionUtils.createActionLongNameNaturalOrder(this.uniqueName, parents);
		this.outputPath = outputPath;
		this.mainClassName = mainClassName;
		this.jobTracker = jobTracker;
		this.nameNode = nameNode;
	}
	
	@Override
	public String getUniqueName() {
		return this.uniqueName;
	}

	@Override
	public List<String> getLongName() {
		return this.longName;
	}

	@Override
	public String getOutputPath() {
		return this.outputPath;
	}
	
	
	public String getMainClassName() {
		return mainClassName;
	}

	public String getJobTracker() {
		return jobTracker;
	}

	public String getNameNode() {
		return nameNode;
	}

	@Override
	public Document toBson() {
		Document toReturn = super.toBson();
		toReturn.append("mainClassName", this.getMainClassName());
		toReturn.append("jobTracker", this.getJobTracker());
		toReturn.append("nameNode", this.getNameNode());
		return toReturn;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((mainClassName == null) ? 0 : mainClassName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		CommandLineAction other = (CommandLineAction) obj;
		if (mainClassName == null) {
			if (other.mainClassName != null)
				return false;
		} else if (!mainClassName.equals(other.mainClassName))
			return false;
		return true;
	}
	
	

}
