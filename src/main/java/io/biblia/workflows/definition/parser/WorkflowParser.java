package io.biblia.workflows.definition.parser;

import io.biblia.workflows.definition.InvalidWorkflowException;
import io.biblia.workflows.definition.Workflow;

public interface WorkflowParser {
	public Workflow parseWorkflow(String workflowString) throws WorkflowParseException, InvalidWorkflowException ;
}
