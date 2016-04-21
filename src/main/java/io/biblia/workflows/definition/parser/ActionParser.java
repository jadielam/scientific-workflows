package io.biblia.workflows.definition.parser;

import io.biblia.workflows.definition.ManagedAction;
import org.bson.Document;

public abstract class ActionParser implements ActionNameConstants {

	/**
	 * Given a JSONObject, it parses it and returns an action
	 * @param object
	 * @return
	 * @throws WorkflowParseException
	 */
	public abstract ManagedAction parseAction(Document object) throws WorkflowParseException;
}
