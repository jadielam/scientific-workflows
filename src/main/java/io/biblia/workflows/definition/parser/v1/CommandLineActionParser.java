package io.biblia.workflows.definition.parser.v1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.google.common.base.CharMatcher;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.actions.CommandLineAction;
import io.biblia.workflows.definition.parser.ActionNameConstants;
import io.biblia.workflows.definition.parser.WorkflowParseException;

public class CommandLineActionParser extends io.biblia.workflows.definition.parser.ActionParser {

	private static CommandLineActionParser instance;
	
	private CommandLineActionParser() {
		
	}
	
	public static CommandLineActionParser getInstance() {
		if (null == instance) {
			instance = new CommandLineActionParser();
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
		if (!type.equals(COMMAND_LINE_ACTION)) {
			throw new WorkflowParseException("The action type: "+ type + " cannot be parsed by CommandLineActionParser");
		}
		String name = (String) actionObject.get("name");
		if (null == name) throw new WorkflowParseException("The action does not have a name");
		String mainClassName = (String) actionObject.get("mainClasName");
		if (null == mainClassName) throw new WorkflowParseException("The action does not have a mainClassName");
		
		Set<String> parentActionNames = this.getParentActionNames(actionObject);
		List<String> inputParameters = this.getInputParameters(actionObject);
		List<String> outputParameters = this.getOutputParameters(actionObject);
		List<String> configurationParameters = this.getConfigurationParameters(actionObject);
		
		return new CommandLineAction(name, 
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
	
	private List<String> getInputParameters(JSONObject actionObject) {
		List<String> toReturn = new ArrayList<String>();
		JSONArray inputParameters = (JSONArray) actionObject.get("inputParameters");
		if (null == inputParameters) {
			return Collections.unmodifiableList(toReturn);
		}
		Iterator<JSONObject> inputParametersIt = inputParameters.iterator();
		while(inputParametersIt.hasNext()) {
			JSONObject inputParameterObject = inputParametersIt.next();
			String value = (String) inputParameterObject.get("value");
			if (null == value) {
				continue;
			}
			toReturn.add(value);
		}
		return Collections.unmodifiableList(toReturn);
	}
	
	private List<String> getOutputParameters(JSONObject actionObject) {
		List<String> toReturn = new ArrayList<String>();
		JSONArray outputParameters = (JSONArray) actionObject.get("outputParameters");
		if (null == outputParameters) {
			return Collections.unmodifiableList(toReturn);
		}
		Iterator<JSONObject> outputParametersIt = outputParameters.iterator();
		while(outputParametersIt.hasNext()) {
			JSONObject outputParametersObject = outputParametersIt.next();
			String value = (String) outputParametersObject.get("value");
			if (null == value) {
				continue;
			}
			toReturn.add(value);
		}
		return Collections.unmodifiableList(toReturn);
	}
	
	/**
	 * If the configuration parameter has spaces.
	 * @param actionObject
	 * @return
	 * @throws WorkflowParseException
	 */
	private List<String> getConfigurationParameters(JSONObject actionObject) throws WorkflowParseException {
		List<String> toReturn = new ArrayList<String>();
		JSONArray configurationParameters = (JSONArray) actionObject.get("configurationParameters");
		if (null == configurationParameters) {
			return Collections.unmodifiableList(toReturn);
		}
		Iterator<JSONObject> configurationParametersIt = configurationParameters.iterator();
		while(configurationParametersIt.hasNext()) {
			JSONObject configurationParametersObject = configurationParametersIt.next();
			String value = (String) configurationParametersObject.get("value");
			if (null == value) {
				continue;
			}
			if (CharMatcher.WHITESPACE.matchesAnyOf(value)) {
				throw new WorkflowParseException("The configuration parameter: \"" + value + "\" has spaces");
			}
			toReturn.add(value);
		}
		return Collections.unmodifiableList(toReturn);
	}
	

}
