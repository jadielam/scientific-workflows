package io.biblia.workflows.definition.parser;

import com.google.common.base.Preconditions;
import io.biblia.workflows.definition.InvalidWorkflowException;
import io.biblia.workflows.definition.Workflow;
import org.bson.Document;
import org.bson.json.JsonParseException;

import java.util.HashMap;
import java.util.Map;

public class BaseWorkflowParser implements WorkflowParser {

	private static Map<String, WorkflowParser> registeredParsers;
	
	static {
		BaseWorkflowParser.registeredParsers = new HashMap<String, WorkflowParser>();
		BaseWorkflowParser.registeredParsers.put("1.0", 
			io.biblia.workflows.definition.parser.v1.WorkflowParser.getInstance());	
	}
	
	/**
	 * Given a json object serialized into a string with the definition of a workflow,
	 * it parses the JSON object into a Workflow.  The first thing that it checks is
	 * the version of the workflow, in order to hand the parsing to the appropriate parser
	 * 
	 * 
	 * @throws WorkflowParseException If the workflowString is not a properly formatted 
	 * or if the JSON definition is missing required attributes.
	 * @throws InvalidWorkflowException if the workflow definition is invalid (Aka: the work
	 * flow has cycles, or actions that are referenced are not defined, etc.)
	 */
	public Workflow parseWorkflow(String workflowString) throws WorkflowParseException, InvalidWorkflowException{
		
		//1. Check version and call the right version depending on parser		
		//2. Parse the object and return a workflow object.


		try{
			Document workflowObj = Document.parse(workflowString);
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
		catch (JsonParseException ex) {
			throw new WorkflowParseException("Error parsing the workflow definition JSON");
		}
		catch(NullPointerException ex) {
			throw new WorkflowParseException(ex.getMessage());
		}
		
	}
	
}
