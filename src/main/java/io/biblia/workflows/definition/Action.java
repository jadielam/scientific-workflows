package io.biblia.workflows.definition;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

/**
 * Represents the definition of a workflow action.
 * @author jadiel
 *
 */
public abstract class Action {

	private final String name;
	private final boolean forceComputation;
	
	public Action(String name, boolean forceComputation) {
		Preconditions.checkNotNull(name);
		this.name = name;
		this.forceComputation = forceComputation;
	}
	/**
	 * Returns the name of the action
	 * @return
	 */
	public String getName() {
		return this.name;
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
	
	/**
	 * Returns a list of the parent action names
	 * @return
	 */
	public abstract Set<String> getParentActionNames();
	
	/**
	 * Returns a list of input paramete paths.
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
