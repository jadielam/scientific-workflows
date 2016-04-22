package io.biblia.workflows.definition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;

import org.bson.Document;

import com.google.common.base.Preconditions;

public abstract class Action {

	private final String originalName;
	
	private final String actionFolder;
	
	private final ActionType type;
	
	private final LinkedHashSet<String> inputPaths;
	
	private final LinkedHashMap<String, String> additionalInput;
	
	private final LinkedHashMap<String, String> configuration;
	
	private final List<Action> parents;
	
	private final boolean forceComputation;
	
	private final boolean isManaged;
	
	public Action(
		String name,
		String actionFolder,
		ActionType type,
		LinkedHashMap<String, String> additionalInput,
		LinkedHashMap<String, String> configuration,
		List<Action> parents,
		boolean forceComputation,
		boolean isManaged
		) throws InvalidWorkflowException
	{
		//1. Validation
		Preconditions.checkNotNull(name);
		Preconditions.checkNotNull(actionFolder);
		Preconditions.checkNotNull(type);
		Preconditions.checkNotNull(parents);
		Preconditions.checkNotNull(configuration);
		Preconditions.checkNotNull(additionalInput);
		
		//2. Setting of fields.
		this.originalName = name;
		this.actionFolder = actionFolder;
		this.type = type;
		this.parents = parents;
		this.configuration = new LinkedHashMap<>(configuration);
		this.additionalInput = new LinkedHashMap<>(additionalInput);
		this.inputPaths = new LinkedHashSet<>();
		
		//1.1 Checking that no two parents are equal
		for (Action parent : parents) {
			String outputPath = parent.getOutputPath();
			if (inputPaths.contains(outputPath)) {
				throw new InvalidWorkflowException("Action " + originalName + " has repeated parents"); 
			}
			inputPaths.add(outputPath);
		}
		this.forceComputation = forceComputation;
		this.isManaged = true;
	}
	
	
	public String getOriginalName(){
		return this.originalName;
	}
	
	public abstract String getUniqueName();
	
	public abstract List<String> getLongName();
	
	public abstract String getOutputPath();
	
	public String getActionFolder() {
		return this.actionFolder;
	}
	
	public List<Action> getParents() {
		return this.parents;
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
	
	public LinkedHashSet<String> getInputPaths() {
		return this.inputPaths;
	}
	
	public Document toBson() {
		Document toReturn = new Document();
		toReturn.append("originalName", this.getOriginalName());
		toReturn.append("uniqueName", this.getUniqueName());
		toReturn.append("longName", this.getLongName());
		toReturn.append("outputPath", this.getOutputPath());
		toReturn.append("actionFolder", this.getActionFolder());
		
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
		
		toReturn.append("extraInput", extraInput);
		toReturn.append("configuration", configurationParameters);
		toReturn.append("type", type);
		toReturn.append("forceComputation", this.forceComputation());
		toReturn.append("isManaged", this.isManaged());
		toReturn.append("inputPaths", new ArrayList<String>(this.getInputPaths()));
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
