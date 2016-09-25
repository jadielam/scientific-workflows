package io.biblia.workflows.definition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.bson.Document;

import com.google.common.base.Preconditions;

import io.biblia.workflows.ConfigurationKeys;

public abstract class Action implements ActionAttributesConstants, ConfigurationKeys {

	/**
	 * THe name given to the action in the workflow submitted to
	 * the system.  More than one action in a workflow can share the 
	 * same original name.
	 */
	private final String originalName;
	
	/**
	 * THe action id given to the action in the workflow submitted
	 * to the system. The action id is unique per workflow.
	 */
	private final Integer actionId;
	
	/**
	 * The action long name is the most unique identifier of this action.
	 * It encodes in it the action unique name of its parents, together
	 * with this action own unique name.
	 * 
	 * For example, given the graph: a1 -> a2 -> a3
	 * Then the long name of a3 is [a1_uniquename, a2_uniquename, a3_uniquename]
	 * 
	 * Trouble comes when one action has more than one parent
	 * Then, the action long name only has two levels:
	 * [hashcode, a3_uniquename]
	 */
	List<String> longName;
	
	/**
	 * The input paths of the action.  I am using a map because the inputs
	 * can then be identified by keys. I am using a LinkedHashMap, because
	 * the order they are inserted could be important, specifically in the 
	 * case of command line actions, where the order in which you pass 
	 * the parameters is important.
	 */
	private LinkedHashMap<String, String> inputParameters;
	
	/**
	 * The path to which this action outputs its dataset.
	 */
	String outputPath;
	
	/**
	 * THe HDFS folder where the action lives.
	 */
	private final String actionFolder;
	
	/**
	 * The type of the action
	 */
	private final ActionType type;
	
	/**
	 * Additional input used by the action. This is input that is not 
	 * managed by the system and that the action can access.
	 */
	private final LinkedHashMap<String, String> additionalInput;
	
	/**
	 * Configuration parameters to the action.
	 */
	private final LinkedHashMap<String, String> configuration;
	
	/**
	 * The list of the ids of parents given to you.
	 */
	private final List<Integer> parentIds;
	
	/**
	 * True if the action will be forced to compute regardless of if its 
	 * output already exists. False otherwise (that is, if the system will de-
	 * termine if the action needs to be computed or not).
	 */
	private final boolean forceComputation;
	
	/**
	 * True if the action output is managed by the system. False if the
	 * action output will live outside of the realm of the system.
	 */
	private final boolean isManaged;
	
	public Action(
		String name,
		Integer actionId,
		String actionFolder,
		ActionType type,
		LinkedHashMap<String, String> additionalInput,
		LinkedHashMap<String, String> configuration,
		List<Integer> parentIds,
		boolean forceComputation,
		String outputPath
		) 
	{
		//1. Validation
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(actionId);
		Preconditions.checkNotNull(actionFolder);
		Preconditions.checkNotNull(type);
		Preconditions.checkNotNull(parentIds);
		Preconditions.checkNotNull(configuration);
		Preconditions.checkNotNull(additionalInput);
		
		//2. Setting of fields.
		this.originalName = name;
		this.actionId = actionId;
		this.actionFolder = actionFolder;
		this.type = type;
		this.parentIds = parentIds;
		this.configuration = new LinkedHashMap<>(configuration);
		this.additionalInput = new LinkedHashMap<>(additionalInput);
		this.forceComputation = forceComputation;
		this.isManaged = false;
		this.outputPath = outputPath;
	}
	
	public Action(
			String name,
			int actionId,
			String actionFolder,
			ActionType type,
			LinkedHashMap<String, String> additionalInput,
			LinkedHashMap<String, String> configuration,
			List<Integer> parentIds,
			boolean forceComputation
			) 
		{
			//1. Validation
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(actionFolder);
			Preconditions.checkNotNull(type);
			Preconditions.checkNotNull(parentIds);
			Preconditions.checkNotNull(configuration);
			Preconditions.checkNotNull(additionalInput);
			
			//2. Setting of fields.
			this.originalName = name;
			this.actionId = actionId;
			this.actionFolder = actionFolder;
			this.type = type;
			this.parentIds = parentIds;
			this.configuration = new LinkedHashMap<>(configuration);
			this.additionalInput = new LinkedHashMap<>(additionalInput);
			this.forceComputation = forceComputation;
			this.isManaged = true;
			this.outputPath = null;
		}
	
	public String getOriginalName(){
		return this.originalName;
	}
	
	public abstract String getUniqueName();
	
	public LinkedHashMap<String, String> getInputParameters() {
		return this.inputParameters;
	}
	
	public void setInputParameters(LinkedHashMap<String, String> inputParameters) {
		this.inputParameters = inputParameters;
	}
	
	public List<String> getLongName(){
		if (null == this.longName) {
			throw new IllegalStateException("longName has not been set yet");
		}
		return this.longName;
	}
	
	public abstract void setLongName(List<List<String>> parentLongNames);
	
	public String getOutputPath() {
		if (null == this.outputPath) {
			throw new IllegalStateException("outputPath has not been set yet");
		}
		return this.outputPath;
	}
	
	public Integer getActionId() {
		return this.actionId;
	}
	public String getActionFolder() {
		return this.actionFolder;
	}
	
	public List<Integer> getParentIds() {
		return this.parentIds;
	}
	
	public LinkedHashMap<String, String> getExtraInputs() {
		return this.additionalInput;
	}
	
	public LinkedHashMap<String, String> getConfiguration() {
		return this.configuration;
	}
	
	public ActionType getType() {
		return this.type;
	}
	
	public boolean forceComputation() {
		return this.forceComputation;
	}
	
	public boolean isManaged() {
		return this.isManaged;
	}
	
	public Document toBson() {
		Document toReturn = new Document();
		toReturn.append(ACTION_ORIGINAL_NAME, this.getOriginalName());
		toReturn.append(ACTION_UNIQUE_NAME, this.getUniqueName());
		toReturn.append(ACTION_LONG_NAME, this.getLongName());
		toReturn.append(ACTION_ID, this.getActionId());
		toReturn.append(ACTION_OUTPUT_PATH, this.getOutputPath());
		toReturn.append(ACTION_FOLDER, this.getActionFolder());
		
		List<Document> extraInput = new ArrayList<Document>();
		for (Entry<String, String> e : this.additionalInput.entrySet()) {
			String value = e.getValue();
			String key = e.getKey();
			Document inputParameter = new Document().append("key", key).append("value", value);
			extraInput.add(inputParameter);
		}
		
		List<Document> configurationParameters = new ArrayList<Document>();
		for (Entry<String, String> e : this.configuration.entrySet()) {
			String value = e.getValue();
			String key = e.getKey();
			Document configurationParameter = new Document().append("key", key).append("value", value);
			configurationParameters.add(configurationParameter);
		}
		
		toReturn.append(ACTION_ADDITIONAL_INPUT, extraInput);
		toReturn.append(ACTION_CONFIGURATION_PARAMETERS, configurationParameters);
		toReturn.append(ACTION_TYPE, type);
		toReturn.append(ACTION_FORCE_COMPUTATION, this.forceComputation());
		toReturn.append(ACTION_IS_MANAGED, this.isManaged());
		return toReturn;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((actionId == null) ? 0 : actionId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Action other = (Action) obj;
		if (actionId == null) {
			if (other.actionId != null)
				return false;
		} else if (!actionId.equals(other.actionId))
			return false;
		return true;
	}	
	
}
