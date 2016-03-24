package io.biblia.workflows.definition;

import java.util.List;
import java.util.Set;

/**
 * Represents the definition of a workflow action.
 * @author jadiel
 *
 */
public interface Action {

	/**
	 * Returns the name of the action
	 * @return
	 */
	public String getName();
	
	/**
	 * Returns a list of the parent action names
	 * @return
	 */
	public Set<String> getParentActionNames();
	
	/**
	 * Returns a list of input paramete paths.
	 * A path can be a folder or a file. If a folder
	 * all the files inside that folder are used as input.
	 * @return
	 */
	public List<String> getInputParameters();
	
	/**
	 * Returns a list of the names of the output parameters
	 * that the program uses. They can both be files or folders.
	 * If a folder, it implies that all the files in that
	 * folder will be output.
	 * @return
	 */
	public List<String> getOutputParameters();
}
