package io.biblia.workflows.definition.parser.v1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.google.common.base.CharMatcher;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.actions.JavaAction;
import io.biblia.workflows.definition.parser.ActionNameConstants;
import io.biblia.workflows.definition.parser.WorkflowParseException;

public class JavaActionParser extends io.biblia.workflows.definition.parser.ActionParser {

	private static JavaActionParser instance;
	
	private JavaActionParser() {
		
	}
	
	public static JavaActionParser getInstance() {
		if (null == instance) {
			instance = new JavaActionParser();
		}
		return instance;
	}

	/**
	 * 2. ACTIONS
	 * 2.1 Components of the action:
	 * 2.1.1 Name and type
	 * 2.1.2 Parent actions.
	 * 2.1.3 Input parameters
	 * 2.1.4 Output parameters
	 * 2.1.5 Configuration parameters:
	 * The value of configuration parameters cannot have spaces.
	 * 2.1.6 Name of the class of the action.
	 * 
	 * Example provided below:
	 * {
	 * 		type: "command-line",
	 * 		name: "testing3",
	 * 		mainClassName: "testing.java",
	 * 		forceComputation: true,
	 * 		parentActions: [
	 * 			{
	 * 				name: "testing1"
	 * 			},
	 * 			{
	 * 				name: "testing2"
	 * 			}
	 * 		],
	 * 		inputParameters: [
	 * 			{	
	 * 				value: "path/to/file"
	 * 			}
	 * 		],
	 * 		outputParameters: [
	 * 			{
	 * 				value: "path/to/file"
	 * 			}
	 * 		
	 * 		],
	 * 		configurationParameters: [
	 * 			{
	 * 				value: "anything"
	 * 			}
	 * 		]
	 * }
	 * @throws WorkflowParseException 
	 */
	@Override
	public Action parseAction(JSONObject actionObject) throws WorkflowParseException {

		String type = (String) actionObject.get("type");
		if (null == type) throw new WorkflowParseException("The action does not have a type attribute");
		if (!type.equals(JAVA_ACTION)) {
			throw new WorkflowParseException("The action type: "+ type + " cannot be parsed by JavaActionParser");
		}
		String name = (String) actionObject.get("name");
		if (null == name) throw new WorkflowParseException("The action does not have a name");
		String mainClassName = (String) actionObject.get("mainClassName");
		if (null == mainClassName) throw new WorkflowParseException("The action does not have a mainClassName");
		Boolean forceComputation = (Boolean) actionObject.get("forceComputation");
		forceComputation = (forceComputation == null || !forceComputation) ? false : true;
		
		Set<String> parentActionNames = this.getParentActionNames(actionObject);
		LinkedHashMap<String, String> inputParameters = this.getInputParameters(actionObject);
		LinkedHashMap<String, String> outputParameters = this.getOutputParameters(actionObject);
		LinkedHashMap<String, String> configurationParameters = this.getConfigurationParameters(actionObject);
		
		return new JavaAction(name, 
				forceComputation, 
				mainClassName, 
				parentActionNames, 
				inputParameters, 
				outputParameters, 
				configurationParameters);
	}
	

	private Set<String> getParentActionNames(JSONObject actionObject) {
		Set<String> toReturn = new HashSet<String>();
		JSONArray parentActions = (JSONArray) actionObject.get("parentActions");
		if (null == parentActions) {
			return Collections.unmodifiableSet(toReturn);
		}
		Iterator<JSONObject> parentActionsIt = parentActions.iterator();
		while(parentActionsIt.hasNext()) {
			JSONObject parentActionObject = parentActionsIt.next();
			String parentActionName = (String) parentActionObject.get("name");
			if (null == parentActionName) {
				continue;
			}
			toReturn.add(parentActionName);
		}
		return Collections.unmodifiableSet(toReturn);
	}
	
	private LinkedHashMap<String, String> getInputParameters(JSONObject actionObject) {
		LinkedHashMap<String, String> toReturn = new LinkedHashMap<>();
		JSONArray inputParameters = (JSONArray) actionObject.get("inputParameters");
		if (null == inputParameters) {
			return toReturn;
		}
		Iterator<JSONObject> inputParametersIt = inputParameters.iterator();
		int counter = 0;
		while(inputParametersIt.hasNext()) {
			counter++;
			JSONObject inputParameterObject = inputParametersIt.next();
			String key = (String) inputParameterObject.get("key");
			String value = (String) inputParameterObject.get("value");
			if (null == value) {
				continue;
			}
			if (null == key) {
				toReturn.put(Integer.toString(counter), value);
			}
			else {
				toReturn.put(key, value);
			}
			
		}
		return toReturn;
	}
	
	private LinkedHashMap<String, String> getOutputParameters(JSONObject actionObject) {
		LinkedHashMap<String, String> toReturn = new LinkedHashMap<>();
		JSONArray outputParameters = (JSONArray) actionObject.get("outputParameters");
		if (null == outputParameters) {
			return toReturn;
		}
		Iterator<JSONObject> outputParametersIt = outputParameters.iterator();
		int counter = 0;
		while(outputParametersIt.hasNext()) {
			counter++;
			JSONObject outputParametersObject = outputParametersIt.next();
			String key = (String) outputParametersObject.get("key");
			String value = (String) outputParametersObject.get("value");
			if (null == value) {
				continue;
			}
			if (null == key) {
				toReturn.put(Integer.toString(counter), value);
			}
			else {
				toReturn.put(key, value);
			}
		}
		return toReturn;
	}
	
	/**
	 * If the configuration parameter has spaces.
	 * @param actionObject
	 * @return
	 * @throws WorkflowParseException
	 */
	private LinkedHashMap<String, String> getConfigurationParameters(JSONObject actionObject) throws WorkflowParseException {
		LinkedHashMap<String, String> toReturn = new LinkedHashMap<>();
		JSONArray configurationParameters = (JSONArray) actionObject.get("configurationParameters");
		if (null == configurationParameters) {
			return toReturn;
		}
		Iterator<JSONObject> configurationParametersIt = configurationParameters.iterator();
		int counter = 0;
		while(configurationParametersIt.hasNext()) {
			counter++;
			JSONObject configurationParametersObject = configurationParametersIt.next();
			String key = (String) configurationParametersObject.get("key");
			String value = (String) configurationParametersObject.get("value");
			if (null == value) {
				continue;
			}
			if (CharMatcher.WHITESPACE.matchesAnyOf(value)) {
				throw new WorkflowParseException("The configuration parameter: \"" + value + "\" has spaces");
			}
			if (null == key) {
				toReturn.put(Integer.toString(counter), value);
			}
			else {
				toReturn.put(key, value);
			}
		}
		return toReturn;
	}
	

}
