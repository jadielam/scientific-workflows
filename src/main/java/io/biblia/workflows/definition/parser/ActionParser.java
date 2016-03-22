package io.biblia.workflows.definition.parser;

import org.json.simple.JSONObject;

import io.biblia.workflows.definition.Action;

public abstract class ActionParser implements ActionNameConstants {

	public abstract Action parseAction(JSONObject object) throws WorkflowParseException;
}
