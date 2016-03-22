package io.biblia.workflows.definition.parser;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.base.Preconditions;

import io.biblia.workflows.definition.InvalidWorkflowException;
import io.biblia.workflows.definition.Workflow;

public class BaseWorkflowParser implements WorkflowParser {

	private static Map<String, WorkflowParser> registeredParsers;
	
	static {
		BaseWorkflowParser.registeredParsers = new HashMap<String, WorkflowParser>();
		BaseWorkflowParser.registeredParsers.put("1.0", 
			io.biblia.workflows.definition.parser.v1.WorkflowParser.getInstance());	
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
	public Workflow parseWorkflow(String workflowString) throws WorkflowParseException, InvalidWorkflowException{
		
		//1. Check version and call the right version depending on parser		
		//2. Parse the object and return a workflow object.
		
		JSONParser jsonParser = new JSONParser();
		try{
			JSONObject workflowObj = (JSONObject) jsonParser.parse(workflowString);
			String version = (String) workflowObj.get("version");
			Preconditions.checkNotNull(version, "The workflow did not include a version attribute.");		
			WorkflowParser wParser = BaseWorkflowParser.registeredParsers.get(version);
			if (null != wParser) {
				Workflow workflow = wParser.parseWorkflow(workflowString);
				return workflow;
			}
			else {
				throw new WorkflowParseException("Could not parse that workflow version");
			}
		}
		catch (ParseException ex) {
			throw new WorkflowParseException("Error parsing the workflow definition JSON");
		}
		catch(NullPointerException ex) {
			throw new WorkflowParseException(ex.getMessage());
		}
		
	}
	
}
