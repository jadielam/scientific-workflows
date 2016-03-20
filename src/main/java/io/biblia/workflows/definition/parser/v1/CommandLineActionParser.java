package io.biblia.workflows.definition.parser.v1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import io.biblia.workflows.definition.Action;

class CommandLineActionParser implements io.biblia.workflows.definition.parser.ActionParser{

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
	 * 2.1 Components of the actions:
	 * 2.1.1 Name and type
	 * 2.1.2 Parent actions.
	 * 2.1.3 Input parameters
	 * 2.1.4 Output parameters
	 * 2.1.5 Configuration parameters.
	 */
	@Override
	public Action parseAction(JSONObject actionObject) {

		String name = (String) actionObject.get("name");
		List<String> parentActionNames = this.getParentActionNames(actionObject);
		List<String> inputParameters = this.getInputParameters(actionObject);
		List<String> outputParameters = this.getOutputParameters(actionObject);
		List<String> configurationParameters = this.getConfigurationParameters(actionObject);
		
		
		return null;
			
	}
	

	private List<String> getParentActionNames(JSONObject actionObject) {
		List<String> toReturn = new ArrayList<String>();
		JSONArray parentActions = (JSONArray) actionObject.get("parentActions");
		if (null == parentActions) {
			return toReturn;
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
		return toReturn;
	}
	
	private List<String> getInputParameters(JSONObject actionObject) {
		List<String> toReturn = new ArrayList<String>();
		JSONArray inputParameters = (JSONArray) actionObject.get("inputParameters");
		if (null == inputParameters) {
			return toReturn;
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
		return toReturn;
	}
	
	private List<String> getOutputParameters(JSONObject actionObject) {
		List<String> toReturn = new ArrayList<String>();
		JSONArray outputParameters = (JSONArray) actionObject.get("outputParameters");
		if (null == outputParameters) {
			return toReturn;
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
		return toReturn;
	}
	
	private List<String> getConfigurationParameters(JSONObject actionObject) {
		List<String> toReturn = new ArrayList<String>();
		JSONArray configurationParameters = (JSONArray) actionObject.get("configurationParameters");
		if (null == configurationParameters) {
			return toReturn;
		}
		Iterator<JSONObject> configurationParametersIt = configurationParameters.iterator();
		while(configurationParametersIt.hasNext()) {
			JSONObject configurationParametersObject = configurationParametersIt.next();
			String value = (String) configurationParametersObject.get("value");
			if (null == value) {
				continue;
			}
			toReturn.add(value);
		}
		return toReturn;
	}
	

}
