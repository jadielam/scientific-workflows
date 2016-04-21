package io.biblia.workflows.definition.parser.v1;

import io.biblia.workflows.definition.ManagedAction;
import io.biblia.workflows.definition.parser.WorkflowParseException;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;

/**
 * Class that parses an action.  It requires that action parsers for
 * specific actions to be registered here in this class.
 * @author jadiel
 *
 */
public class ActionParser extends io.biblia.workflows.definition.parser.ActionParser{
	
	private static Map<String, io.biblia.workflows.definition.parser.ActionParser> registeredParsers;
	
	static {
		ActionParser.registeredParsers = new HashMap<String, io.biblia.workflows.definition.parser.ActionParser>();
		ActionParser.registeredParsers.put(JAVA_ACTION, JavaActionParser.getInstance());
		ActionParser.registeredParsers.put(FS_DELETE_ACTION, FSDeleteParser.getInstance());
	}
	
	
	/**
	 * Obtains an action definition from an action object.
	 * If checks the type parameter of the action to delegate
	 * the job to the specific parser for that action type.
	 * @throws WorkflowParseException if no type parameter is found, or if no action parser
	 * is registered for the type of that action.
	 */
	public ManagedAction parseAction(Document actionObject) throws WorkflowParseException {
		
		String type = (String) actionObject.get("type");
		if (null == type) {
			throw new WorkflowParseException("Action object requires a type parameters");
		}
		io.biblia.workflows.definition.parser.ActionParser actionParser = registeredParsers.get(type);
		if (null == actionParser) {
			throw new WorkflowParseException("No ActionParser registered for the type: " + type);
		}
		
		ManagedAction action = actionParser.parseAction(actionObject);
		return action;
	}
	
}
