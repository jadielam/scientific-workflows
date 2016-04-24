package io.biblia.workflows.definition.parser.v1;

import com.google.common.base.CharMatcher;
import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.ActionAttributesConstants;
import io.biblia.workflows.definition.CommandLineAction;
import io.biblia.workflows.definition.parser.WorkflowParseException;
import org.bson.Document;

import java.util.*;

public class CommandLineActionParser extends io.biblia.workflows.definition.parser.ActionParser 
implements ActionAttributesConstants  {

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
	 * 2. ACTIONS 2.1 Components of the action: 2.1.1 Name and type 2.1.2 Parent
	 * actions. 2.1.3 Input parameters 2.1.4 Output parameters 2.1.5
	 * Configuration parameters: The value of configuration parameters cannot
	 * have spaces. 2.1.6 Name of the class of the action.
	 * 
	 * Example provided below: { type: "command-line", name: "testing3",
	 * mainClassName: "testing.java", jobTracker: "urlTOJobTracker", nameNode:
	 * "urlToNameNode", forceComputation: true, parentActions: [ { name:
	 * "testing1" }, { name: "testing2" } ], additionalInput: [ { value:
	 * "path/to/file" } ], outputParameters: [ { value: "path/to/file" }
	 * 
	 * ], configurationParameters: [ { value: "anything" } ] }
	 * 
	 * @throws WorkflowParseException
	 */
	@Override
	public Action parseAction(Document actionObject) throws WorkflowParseException {

		String type = (String) actionObject.get("type");
		if (null == type)
			throw new WorkflowParseException("The action does not have a type attribute");
		if (!type.equals(COMMAND_LINE_ACTION)) {
			throw new WorkflowParseException("The action type: " + type + " cannot be parsed by CommandLineActionParser");
		}
		String name = (String) actionObject.get(ACTION_NAME);
		if (null == name)
			throw new WorkflowParseException("The action does not have a name");
		String actionFolder = (String) actionObject.get(ACTION_FOLDER);
		if (null == actionFolder)
			throw new WorkflowParseException("The action does not have attribute <actionFolder>");
		String mainClassName = (String) actionObject.get(COMMANDLINE_MAIN_CLASS_NAME);
		if (null == mainClassName)
			throw new WorkflowParseException("The action does not have a mainClassName");
		String nameNode = (String) actionObject.get(COMMANDLINE_NAME_NODE);
		if (null == nameNode)
			throw new WorkflowParseException("The action does not have a nameNode attribute");
		String jobTracker = (String) actionObject.get(COMMANDLINE_JOB_TRACKER);
		if (null == jobTracker)
			throw new WorkflowParseException("The action does not have a jobTracker attribute");
		Boolean forceComputation = (Boolean) actionObject.get(ACTION_FORCE_COMPUTATION);
		forceComputation = (forceComputation == null || !forceComputation) ? false : true;
		String outputFolder = actionObject.getString(ACTION_OUTPUT_PATH);

		List<String> parentActionNames = this.getParentActionNames(actionObject);
		LinkedHashMap<String, String> additionalInput = this.getInputParameters(actionObject);
		LinkedHashMap<String, String> configurationParameters = this.getConfigurationParameters(actionObject);

		return new CommandLineAction(name, 
				actionFolder, 
				additionalInput, 
				configurationParameters,
				parentA
				)
		return new JavaAction(name, type, actionFolder,
				forceComputation, 
				mainClassName, 
				jobTracker, nameNode, 
				parentActionNames,
				inputParameters, 
				outputParameters, 
				configurationParameters);
	}

	private List<String> getParentActionNames(Document actionObject) {
		List<String> toReturn = new ArrayList<String>();
		@SuppressWarnings("unchecked")
		List<Document> parentActions = (List<Document>) actionObject.get(ACTION_PARENT_ACTIONS);
		if (null == parentActions) {
			return Collections.unmodifiableList(toReturn);
		}
		Iterator<Document> parentActionsIt = parentActions.iterator();
		while (parentActionsIt.hasNext()) {
			Document parentActionObject = parentActionsIt.next();
			String parentActionName = (String) parentActionObject.get("name");
			if (null == parentActionName) {
				continue;
			}
			toReturn.add(parentActionName);
		}
		return Collections.unmodifiableList(toReturn);
	}

	private LinkedHashMap<String, String> getInputParameters(Document actionObject) {
		LinkedHashMap<String, String> toReturn = new LinkedHashMap<>();
		@SuppressWarnings("unchecked")
		List<Document> inputParameters = (List<Document>) actionObject.get(ACTION_ADDITIONAL_INPUT);
		if (null == inputParameters) {
			return toReturn;
		}
		Iterator<Document> inputParametersIt = inputParameters.iterator();
		int counter = 0;
		while (inputParametersIt.hasNext()) {
			counter++;
			Document inputParameterObject = inputParametersIt.next();
			String key = (String) inputParameterObject.get("key");
			String value = (String) inputParameterObject.get("value");
			if (null == value) {
				continue;
			}
			if (null == key) {
				toReturn.put(Integer.toString(counter), value);
			} else {
				toReturn.put(key, value);
			}

		}
		return toReturn;
	}

	/**
	 * If the configuration parameter has spaces.
	 * 
	 * @param actionObject
	 * @return
	 * @throws WorkflowParseException
	 */
	private LinkedHashMap<String, String> getConfigurationParameters(Document actionObject)
			throws WorkflowParseException {
		LinkedHashMap<String, String> toReturn = new LinkedHashMap<>();
		@SuppressWarnings("unchecked")
		List<Document> configurationParameters = (List<Document>) actionObject.get(ACTION_CONFIGURATION_PARAMETERS);
		if (null == configurationParameters) {
			return toReturn;
		}
		Iterator<Document> configurationParametersIt = configurationParameters.iterator();
		int counter = 0;
		while (configurationParametersIt.hasNext()) {
			counter++;
			Document configurationParametersObject = configurationParametersIt.next();
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
			} else {
				toReturn.put(key, value);
			}
		}
		return toReturn;
	}

}
