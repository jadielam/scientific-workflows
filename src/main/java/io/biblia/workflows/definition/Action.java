package io.biblia.workflows.definition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import org.bson.Document;

import com.google.common.base.Preconditions;

public abstract class Action implements ActionAttributesConstants {

	private final String originalName;
	
	private final Integer actionId;
	
	private List<String> longName;
	
	private String outputPath;
	
	private final String actionFolder;
	
	private final ActionType type;
	
	private final LinkedHashMap<String, String> additionalInput;
	
	private final LinkedHashMap<String, String> configuration;
	
	private final List<Integer> parentIds;
	
	private final boolean forceComputation;
	
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
	
	public List<String> getLongName(){
		return this.longName;
	}
	
	public void setLongName(List<String> longName) {
		this.longName = longName;
		if (null == this.outputPath) {
			this.outputPath = ActionUtils.generateOutputPathFromLongName(longName);
		}
	}
	
	public String getOutputPath() {
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
		result = prime * result + ((originalName == null) ? 0 : originalName.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((this.getLongName() == null) ? 0 : this.getLongName().hashCode());
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
		if (originalName == null) {
			if (other.originalName != null)
				return false;
		} else if (!originalName.equals(other.originalName))
			return false;
		if (this.getLongName() == null) {
			if (other.getLongName() != null) {
				return false;
			}
		}
		else if (!this.getLongName().equals(other.getLongName())) {
			return false;
		}
		if (type != other.type)
			return false;
		return true;
	}

	
	
}
