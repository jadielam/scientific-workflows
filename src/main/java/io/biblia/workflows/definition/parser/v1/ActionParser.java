package io.biblia.workflows.definition.parser.v1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.parser.ActionNameConstants;
import io.biblia.workflows.definition.parser.WorkflowParseException;

public class ActionParser extends io.biblia.workflows.definition.parser.ActionParser{
	
	private static Map<String, io.biblia.workflows.definition.parser.ActionParser> registeredParsers;
	
	static {
		ActionParser.registeredParsers = new HashMap<String, io.biblia.workflows.definition.parser.ActionParser>();
		ActionParser.registeredParsers.put(COMMAND_LINE_ACTION, CommandLineActionParser.getInstance());
	}
	
	public Action parseAction(JSONObject actionObject) throws WorkflowParseException {
		
		String type = (String) actionObject.get("type");
		if (null == type) {
			throw new WorkflowParseException("Action object requires a type parameters");
		}
		io.biblia.workflows.definition.parser.ActionParser actionParser = registeredParsers.get(type);
		if (null == actionParser) {
			throw new WorkflowParseException("No ActionParser registered for the type: " + type);
		}
		
		Action action = actionParser.parseAction(actionObject);
		return action;
	}
	
}
