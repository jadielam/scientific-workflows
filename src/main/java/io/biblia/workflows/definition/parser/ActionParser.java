package io.biblia.workflows.definition.parser;

import org.json.simple.JSONObject;

import io.biblia.workflows.definition.Action;

public interface ActionParser {

	public Action parseAction(JSONObject object) throws WorkflowParseException;
}
