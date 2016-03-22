package io.biblia.workflows.definition.parser.v1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Preconditions;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.Workflow;
import io.biblia.workflows.definition.parser.WorkflowParseException;

public class WorkflowParser implements io.biblia.workflows.definition.parser.WorkflowParser {

	private static WorkflowParser instance = null;
	private final ActionParser actionParser;
	
	private WorkflowParser() {
		this.actionParser = new ActionParser();
	}

	/**
	 * 1. WORKFLOW: 
	 * 1.1 Components of the workflow:
	 * 1.1.1 Name
	 * 1.1.2 (TODO) Global configuration parameters.
	 * 1.1.3 Start action
	 * 1.1.4 End action
	 * 1.1.5 Version, so that I know which parser to pick up.
	 * 
	 * 2. ACTIONS
	 * 2.1 Components of the actions:
	 * 2.1.1 Name and type
	 * 2.1.2 Parent actions.
	 * 2.1.3 Input parameters
	 * 2.1.4 Output parameters
	 * 2.1.5 Configuration parameters.
	 * 
	 * 2.2 Notes on the actions
	 * 2.2.1 An action will not be executed until all its parent actions 
	 * have been executed first.
	 * 2.2.2 A validator will need to check that there are no cycles in the DAG
	 * that is formed by the dependency net of the actions.
	 * 2.2.3 The input file parameters implicitly define a dependency on actions too.
	 * For example, if the an action A1 has file/folder F1 as input, and action A2 has
	 * file/folder F1 as output, then action A1 implicitly depends on action A2
	 * even if action A2 is not among the parent actions of action A1.
	 * 
	 * 2.2.4 The JSON structure. I don't need to have forks and joins.  I don't need
	 * to have if-else statements. In the future I might throw them in. Right now, all 
	 * I care is about the dependencies.
	 * 
	 *  3. How will I handle versioning and action types:
	 *  
	 * 
	 * @param workflow
	 * @return
	 * @throws ParseException If the workflowString is not a properly formatted
	 * JSON object
	 * @throws 
	 */
	public Workflow parseWorkflow(String workflowString) throws WorkflowParseException {
		
		JSONParser jsonParser = new JSONParser();
		try{
			JSONObject workflowObj = (JSONObject) jsonParser.parse(workflowString);
			String name = (String) workflowObj.get("name");
			if (null == name) throw new WorkflowParseException("The workflow did not include any name");
			Preconditions.checkNotNull(name, "The workflow did not include a name attribute.");
			
			//1. Get start action name
			JSONObject startActionObject = (JSONObject) workflowObj.get("startAction");
			if (null == startActionObject) throw new WorkflowParseException("The workflow definition did not have start action");
			String startActionName = (String) workflowObj.get("name");
			if (null == startActionName) throw new WorkflowParseException("The workflow definition did not have start action name");
			
			//2. Get end action name
			JSONObject endActionObject = (JSONObject) workflowObj.get("endAction");
			if (null == endActionObject) throw new WorkflowParseException("The workflow definition did not have end action");
			String endActionName = (String) workflowObj.get("name");
			if (null == endActionName) throw new WorkflowParseException("The workflow definition did not have end action name");
			
			//3. Get actions
			JSONArray actionsListObject = (JSONArray) workflowObj.get("actions");
			Iterator<JSONObject> actionObjectsIt = actionsListObject.iterator();
			List<Action> actions = new ArrayList<Action>();
			while (actionObjectsIt.hasNext()) {
				JSONObject actionObject = actionObjectsIt.next();
				Action action = actionParser.parseAction(actionObject);
				actions.add(action);
			}
			
			
			//4. Create workflow object with a DAG
		}
		catch(ParseException ex) {
			throw new WorkflowParseException("Error parsing the workflow JSON object");
		}
		
		return null;
	}
	
	public static WorkflowParser getInstance() {
		if (null == instance) {
			instance = new WorkflowParser();
		}
		return instance;
	}
}
