package io.biblia.workflows.definition;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Preconditions;

public class WorkflowParser {

	/**
	 * 1. WORKFLOW: 
	 * 1.1 Components of the workflow:
	 * 1.1.1 Name
	 * 1.1.2 (TODO) Global configuration parameters.
	 * 1.1.3 Start action
	 * 1.1.4 End action
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
	 * The JSON structure is the following:
	 * 
	 * @param workflow
	 * @return
	 * @throws ParseException If the workflowString is not a properly formatted
	 * JSON object
	 * @throws 
	 */
	public static Workflow parseWorkflow(String workflowString) throws ParseException {
		JSONParser parser = new JSONParser();
		try{
			JSONObject workflowObj = (JSONObject) parser.parse(workflowString);
			String wName = (String) workflowObj.get("name");
			Preconditions.checkNotNull(wName, "The workflow did not include a name attribute.");
			
			
			
		}
		catch (ParseException ex) {
			throw ex;
		}
		
		return null;
	}
	
}
