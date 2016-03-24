package io.biblia.workflows.definition.parser;

import org.json.simple.JSONObject;

import io.biblia.workflows.definition.Action;

public abstract class ActionParser implements ActionNameConstants {

	/**
	 * Given a JSONObject, it parses it and returns an action
	 * @param object
	 * @return
	 * @throws WorkflowParseException
	 */
	public abstract Action parseAction(JSONObject object) throws WorkflowParseException;
}
