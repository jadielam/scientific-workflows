package io.biblia.workflows.definition.actions;

import com.google.common.base.Preconditions;
import io.biblia.workflows.definition.Action;
import org.bson.Document;

import java.util.*;
import java.util.Map.Entry;

/**
 * Defines a command line action.
 * A command line action takes input parameters, 
 * output parameters and configuration parameters.
 * 
 * To the command line actions all this parameters are 
 * passed as command line parameters, in the following order:
 * 1. First input parameters, in the order defined by the list.
 * 2. Then output parameters, in the order defined by the list.
 * 3. Then configuration parameters, in the order defined by the list.
 * @author jadiel
 *
 */
public class JavaAction extends Action {

	private final String mainClassName;
	private final String jobTracker;
	private final String nameNode;
	private final Set<String> parentActionNames;
	private final Map<String, String> inputParameters;
	private final Map<String, String> outputParameters;
	private final Map<String, String> configurationParameters;
	
	public JavaAction(String name,
					  String type,
			String actionFolder,
			boolean forceComputation, 
			String mainClassName,
			String jobTracker,
			String nameNode,
			Set<String> parentActionNames,
			LinkedHashMap<String, String> inputParameters, 
			LinkedHashMap<String, String> outputParameters, 
			LinkedHashMap<String, String> configurationParameters) {
	
		super(name, type, actionFolder, forceComputation);
		Preconditions.checkNotNull(mainClassName);
		Preconditions.checkNotNull(jobTracker);
		Preconditions.checkNotNull(nameNode);
		Preconditions.checkNotNull(parentActionNames);
		Preconditions.checkNotNull(inputParameters);
		Preconditions.checkNotNull(outputParameters);
		Preconditions.checkNotNull(configurationParameters);
		this.mainClassName = mainClassName;
		this.jobTracker = jobTracker;
		this.nameNode = nameNode;
		this.parentActionNames = parentActionNames;
		this.inputParameters = Collections.unmodifiableMap(inputParameters);
		this.outputParameters = Collections.unmodifiableMap(outputParameters);
		this.configurationParameters = Collections.unmodifiableMap(configurationParameters);
	}

	public String getNameNode() {
		return this.nameNode;
	}
	
	public String getJobTracker() {
		return this.jobTracker;
	}
	
	public String getMainClassName() {
		return mainClassName;
	}
	
	public Set<String> getParentActionNames() {
		return parentActionNames;
	}

	public Map<String, String> getInputParameters() {
		return inputParameters;
	}

	public Map<String, String> getOutputParameters() {
		return outputParameters;
	}

	public Map<String, String> getConfigurationParameters() {
		return configurationParameters;
	}

	@Override
	public Document toBson() {
		Document document = super.toBson();
		document.append("mainClass", this.mainClassName);
		document.append("jobTracker", this.jobTracker);
		document.append("nameNode", this.nameNode);
		document.append("parentActionNames", this.parentActionNames);
		document.append("inputParameters", this.convertToDocumentsList(this.inputParameters));
		document.append("outputParameters", this.convertToDocumentsList(this.outputParameters));
		document.append("configurationParameters", this.convertToDocumentsList(this.configurationParameters));
		return document;
	}

	private List<Document> convertToDocumentsList(Map<String, String> parameters) {
		List<Document> toReturn = new ArrayList<Document>();
		Set<Entry<String, String>> entrySet = parameters.entrySet();
		for (Entry<String, String> e : entrySet) {
			String key = e.getKey();
			String value = e.getValue();
			Document toAdd = new Document(key, value);
			toReturn.add(toAdd);
		}
		return toReturn;
	}
}
