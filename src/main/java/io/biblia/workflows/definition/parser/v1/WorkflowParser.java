package io.biblia.workflows.definition.parser.v1;

import com.google.common.base.Preconditions;
import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.InvalidWorkflowException;
import io.biblia.workflows.definition.Workflow;
import io.biblia.workflows.definition.parser.WorkflowParseException;
import org.bson.Document;
import org.bson.json.JsonParseException;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WorkflowParser implements io.biblia.workflows.definition.parser.WorkflowParser {

	private static WorkflowParser instance = null;
	private final ActionParser actionParser;
	
	private WorkflowParser() {
		this.actionParser = new ActionParser();
	}

	/**
	 * @throws InvalidWorkflowException 
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
	 * The version 1 workflow looks like the following:
	 * {
	 * 		name: "Workflow Name",
	 * 		version: "1.0",
	 * 		startActionId: 1
	 * 		endActionId: 2
	 *		actions: [
	 *			{
	 *				
	 *			},
	 *			{
	 *			
	 *			}
	 *		]
	 * 		
	 * 			
	 * 
	 * 
	 * }
	 * @param workflowString
	 * @return the workflow parsed
	 * @throws ParseException If the workflowString is not a properly formatted
	 * JSON object
	 * @throws 
	 */
	public Workflow parseWorkflow(String workflowString) throws WorkflowParseException, InvalidWorkflowException {

		try{

			Document workflowObj = Document.parse(workflowString);
			String name = (String) workflowObj.get("name");
			if (null == name) throw new WorkflowParseException("The workflow did not include any name");
			Preconditions.checkNotNull(name, "The workflow did not include a name attribute.");
			
			//1. Get start action name
			Integer startActionId = workflowObj.getInteger("startActionId");
			if (null == startActionId) throw new WorkflowParseException("The workflow definition did not have start action");
			
			//2. Get end action name
			Integer endActionId = workflowObj.getInteger("endActionId");
			if (null == endActionId) throw new WorkflowParseException("The workflow definition did not have end action");
		
			//3. Get actions
			@SuppressWarnings("unchecked")
			List<Document> actionsListObject = (List<Document>) workflowObj.get("actions");
			Iterator<Document> actionObjectsIt = actionsListObject.iterator();
			List<Action> actions = new ArrayList<>();
			while (actionObjectsIt.hasNext()) {
				Document actionObject = actionObjectsIt.next();
				Action action = actionParser.parseAction(actionObject);
				actions.add(action);
			}	
			
			//4. Create workflow object with a DAG
			return new Workflow(name, startActionId, endActionId, actions);
			
		}
		catch(JsonParseException ex) {
			throw new WorkflowParseException("Error parsing the workflow JSON object");
		}
	}
	
	public static WorkflowParser getInstance() {
		if (null == instance) {
			instance = new WorkflowParser();
		}
		return instance;
	}
}
