package io.biblia.workflows.definition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.bson.Document;

/**
 * The difference between an unmanaged action and an action
 * is that forceComputation is automatically set to true,
 * and the output needs to be passed explicitly to the
 * constructor.  
 * @author dearj019
 *
 */
public class UnmanagedAction implements Action {

	private final Set<Action> parents;
	
	private final String originalName;
	
	private final String uniqueName;
	
	private final List<String> longName;
	
	private final boolean forceComputation = true;
	
	private final String actionFolder;
	
	private final ActionType type;
	
	private final LinkedHashMap<String, String> additionalInput;
	
	private final LinkedHashMap<String, String> configuration;
	
	private final String outputPath;
	
	private final List<String> inputPaths;
	public UnmanagedAction(
		String name,
		String actionFolder,
		ActionType type,
		LinkedHashMap<String, String> additionalInput,
		Set<Action> parents,
		LinkedHashMap<String, String> configuration,
		String outputPath
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
		return this.longName;
	}

	@Override
	public String getOutputPath() {
		return this.outputPath;
	}

	@Override
	public LinkedHashMap<String, String> getExtraInputs() {
		return this.additionalInput;
	}

	@Override
	public LinkedHashMap<String, String> getConfiguration() {
		return this.configuration;
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
		return this.inputPaths;
	}

	@Override
	public Document toBson() {
		//TODO
		return null;
	}

	
}
