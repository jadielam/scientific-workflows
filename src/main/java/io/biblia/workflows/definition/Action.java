package io.biblia.workflows.definition;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.Set;

/**
 * Represents the definition of a workflow action.
 * @author jadiel
 *
 */
public abstract class Action {

	private final String name;
	private final String actionFolder;
	private final boolean forceComputation;
	private final String type;
	
	public Action(String name, String type,
				  String actionFolder,
				  boolean forceComputation) {
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(type);
		this.name = name;
		this.type = type;
		this.actionFolder = actionFolder;
		this.forceComputation = forceComputation;
	}
	/**
	 * Returns the name of the action
	 * @return
	 */
	public String getName() {
		return this.name;
	}
	
	public String getActionFolder() {
		return this.actionFolder;
	}
	/**
	 * Returns if this computation will be forced
	 * to be exeuted, regardless of the presence
	 * or not presence of datasets.
	 * @return
	 */
	public boolean getForceComputation() {
		return this.forceComputation;
	}

	public String getType() {
		return type;
	}

	/**
	 * Returns a list of the parent action names
	 * @return
	 */
	public abstract Set<String> getParentActionNames();
	
	/**
	 * Returns a list of input parameter paths.
	 * A path can be a folder or a file. If a folder
	 * all the files inside that folder are used as input.
	 * @return
	 */
	public abstract Map<String, String> getInputParameters();
	
	/**
	 * Returns a list of the names of the output parameters
	 * that the program uses. They can both be files or folders.
	 * If a folder, it implies that all the files in that
	 * folder will be output.
	 * @return
	 */
	public abstract Map<String, String> getOutputParameters();
	
	public abstract Map<String, String> getConfigurationParameters();
}
