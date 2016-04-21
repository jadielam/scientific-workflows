package io.biblia.workflows.definition.parser.v1;

import org.bson.Document;

import io.biblia.workflows.definition.ManagedAction;
import io.biblia.workflows.definition.actions.FSDeleteAction;
import io.biblia.workflows.definition.parser.WorkflowParseException;

public class FSDeleteParser extends io.biblia.workflows.definition.parser.ActionParser {

	private static FSDeleteParser instance;
	
	private FSDeleteParser() {
		
	}
	
	public static FSDeleteParser getInstance() {
		if (null == instance) {
			instance = new FSDeleteParser();
		}
		return instance;
	}
	
	@Override
	public ManagedAction parseAction(Document aObject) throws WorkflowParseException {
		String type = (String) aObject.get("type");
		if (null == type) {
			throw new WorkflowParseException("The action does not have a type attribute");
		}
		if (!type.equals(FS_DELETE_ACTION)) {
			throw new WorkflowParseException("The action type: " + type + " cannot be parsed by FSDeleteParser");
		}
		String name = aObject.getString("name");
		if (null == name) {
			throw new WorkflowParseException("The action does not have a name");
		}
		String actionFolder = aObject.getString("actionFolder");
		if (null == actionFolder) {
			throw new WorkflowParseException("The action does not have attribute <actionFolder>");
		}
		String deletePath = aObject.getString("pathToDelete");
		if (null == deletePath) {
			throw new WorkflowParseException("The action does not have attribute <pathToDelete>");
		}
		
		return new FSDeleteAction(name, actionFolder, deletePath);
	}
	

}
