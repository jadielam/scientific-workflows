package io.biblia.workflows.definition.parser;

import org.json.simple.parser.ParseException;

import io.biblia.workflows.definition.Workflow;

public interface WorkflowParser {

	public Workflow parseWorkflow(String workflowString) throws WorkflowParseException ;
}
