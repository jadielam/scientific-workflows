package biblia.io.workflows.definition.parser;

import org.junit.Test;

import biblia.io.workflows.InputReader;
import io.biblia.workflows.definition.parser.BaseWorkflowParser;
import io.biblia.workflows.definition.parser.WorkflowParser;
import junit.framework.TestCase;

public class TestWorkflowParser extends TestCase {

	private WorkflowParser parser = new BaseWorkflowParser();
	
	protected void setUp() throws Exception {
		super.setUp();
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testName() throws Exception {
		String json = InputReader.getFile("workflows/good1.txt");
		this.parser.parseWorkflow(json);
	}

}
