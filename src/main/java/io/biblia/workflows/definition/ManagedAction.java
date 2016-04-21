package io.biblia.workflows.definition;

import com.google.common.base.Preconditions;
import org.bson.Document;
import java.util.LinkedHashMap;

import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
/**
 * Represents the definition of a workflow action.
 * @author jadiel
 *
 */
public class ManagedAction implements Action{

	/**
	 * Contains the name of an action.
	 * THe original name given in the configuration file
	 */
	private final String originalName;
	
	/**
	 * The unique name of the action is an encryption 
	 * of the original name together with the additionalInput and
	 * configuration. Notice that uniqueName depends on the
	 * order on which the configuration parameters are given.
	 * TODO: Important to document this. This is what will make
	 * me have to look into the type of the action to determine
	 * how to go about it.
	 */
	private final String uniqueName;
	
	/**
	 * Long name is a unique name formed recursively by adding the 
	 * uniqueName of the action to the longName of the parents.
	 */
	private final List<String> longName;
	
	/**
	 * The HDFS folder where the jar files and xml configuration files are.
	 */
	private final String actionFolder;
	
	/**
	 * True if the action will be computed regardless of the presence of its 
	 * input.  
	 * False if the system will manage if the action is computed or not.
	 */
	private final boolean forceComputation;
	
	/**
	 * THe type of the action
	 */
	private final ActionType type;
	
	/**
	 * Parents on whose output this action depends.
	 */
	private final Set<Action> parents;
	
	private final List<String> inputPaths;
	
	/**
	 * Additional input that the action might need.  This input
	 * is generally from paths that are not managed by the system.
	 */
	private final LinkedHashMap<String, String> additionalInput;
	
	/**
	 * Configuration parameters of the action
	 */
	private final LinkedHashMap<String, String> configuration;
	
	/**
	 * Output path.
	 */
	private final String outputPath;

	public ManagedAction(
		String name,
		String actionFolder,
		boolean forceComputation,
		ActionType type,
		LinkedHashMap<String, String> additionalInput,
		Set<Action> parents,
		LinkedHashMap<String, String> configuration
		
			) {
		
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(actionFolder);
		Preconditions.checkNotNull(type);
		Preconditions.checkNotNull(additionalInput);
		Preconditions.checkNotNull(parents);
		Preconditions.checkNotNull(configuration);
		this.parents = new HashSet<Action>(parents);
		this.originalName = name;
		this.uniqueName = ActionUtils.createActionUniqueName(this.originalName, additionalInput, configuration);
		this.longName = ActionUtils.createActionLongName(uniqueName, parents);
		this.actionFolder = actionFolder;
		this.forceComputation = forceComputation;
		this.type = type;
		this.additionalInput = new LinkedHashMap<String, String>(additionalInput);
		this.configuration = new LinkedHashMap<String, String>(configuration);
		this.outputPath = ActionUtils.generateOutputPathFromLongName(this.longName);
		this.inputPaths = new ArrayList<>();
		for (Action parent : parents) {
			inputPaths.add(parent.getOutputPath());
		}
		
	}
	


	@Override
	public String getOriginalName() {
		return this.originalName;
	}

	@Override
	public String getUniqueName() {
		return this.uniqueName;
	}

	@Override
	public List<String> getLongName() {
		return new ArrayList<>(this.longName);
	}

	@Override
	public String getOutputPath() {
		return this.outputPath;
	}

	@Override
	public LinkedHashMap<String, String> getExtraInputs() {
		return new LinkedHashMap<>(this.additionalInput);
	}

	@Override
	public LinkedHashMap<String, String> getConfiguration() {
		return new LinkedHashMap<>(this.configuration);
	}

	@Override
	public ActionType getType() {
		return this.type;
	}

	@Override
	public boolean forceComputation() {
		return this.forceComputation;
	}

	@Override
	public List<String> getInputPaths() {
		return Collections.unmodifiableList(this.inputPaths);
	}

	/**
	 * Will be used for the database representation of the document.
	 */
	@Override
	public Document toBson() {
		// TODO Auto-generated method stub
		return null;
	}}
