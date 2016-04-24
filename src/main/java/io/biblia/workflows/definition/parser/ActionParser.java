package io.biblia.workflows.definition.parser;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.ActionNameConstants;

import org.bson.Document;

public abstract class ActionParser implements ActionNameConstants {

	/**
	 * Given a JSONObject, it parses it and returns an action
	 * @param object
	 * @return
	 * @throws WorkflowParseException
	 */
	public abstract Action parseAction(Document object) throws WorkflowParseException;
}
