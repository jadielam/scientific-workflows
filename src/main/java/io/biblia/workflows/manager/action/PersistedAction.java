package io.biblia.workflows.manager.action;

import java.util.List;
import com.google.common.base.Preconditions;

import io.biblia.workflows.definition.Action;
import io.biblia.workflows.definition.parser.WorkflowParseException;
import io.biblia.workflows.definition.parser.v1.ActionParser;

import org.bson.Document;
import org.bson.json.JsonParseException;
import org.bson.types.ObjectId;

import java.util.Date;

/**
 * Created by dearj019 on 4/12/16.
 */
public class PersistedAction {

	/**
	 * The action definition submitted to the
	 * system initially.
	 */
    private final Action action;
    
    /**
     * The id of the entry in mongodb
     */
    private final ObjectId _id;
    
    /**
     * The current state of the action.
     */
    private final ActionState state;
    
    /**
     * Last time the entry was updated
     */
    private final Date lastUpdatedDate;
    
    /**
     * Id that identifies the submission to 
     * Hadoop. It can be null when the
     * action still has not been submitted.
     */
    private String submissionId;
    
    /**
     * An integer used for versioning.
     */
    private int version;
    
    /**
     * Time when the action started being run by Hadoop
     */
    private Date startTime = null;
    
    /**
     * Time when Hadoop (or the runner) finished running the action.
     */
    private Date endTime = null;
    
    private Double sizeInMB;
    
    private Long marker;
    
    private final Long workflowId;
    
    private List<String> parentsActionIds;
    
    private List<String> parentActionOutputs;
    
    private static final io.biblia.workflows.definition.parser.ActionParser PARSER = new ActionParser();

    public PersistedAction(Action action, ObjectId _id,
    						Long workflowId, 
                           ActionState state, Date lastUpdatedDate, 
                           int version, List<String> parentsActionIds,
                           List<String> parentActionOutputs,
                           Double sizeInMB, Long marker) {
        Preconditions.checkNotNull(action);
        Preconditions.checkNotNull(_id);
        Preconditions.checkNotNull(state);
        Preconditions.checkNotNull(lastUpdatedDate);
        this.action = action;
        this._id = _id;
        this.state = state;
        this.lastUpdatedDate = lastUpdatedDate;
        this.workflowId = workflowId;
        this.version = version;
        this.submissionId = null;
        this.parentsActionIds = parentsActionIds;
        this.parentActionOutputs = parentActionOutputs;
        this.sizeInMB = sizeInMB;
        this.marker = marker;
    }
    
    public PersistedAction(Action action, ObjectId _id,
    		Long workflowId, 
            ActionState state, Date lastUpdatedDate, 
            int version, String submissionId, List<String> parentsActionIds,
            List<String> parentActionOutputs,
            Double sizeInMB, Long marker) {
    	Preconditions.checkNotNull(action);
    	Preconditions.checkNotNull(_id);
    	Preconditions.checkNotNull(state);
    	Preconditions.checkNotNull(lastUpdatedDate);
    	this.action = action;
    	this._id = _id;
    	this.state = state;
    	this.lastUpdatedDate = lastUpdatedDate;
    	this.version = version;
    	this.workflowId = workflowId;
    	this.submissionId = submissionId;
    	this.parentsActionIds = parentsActionIds;
    	this.parentActionOutputs = parentActionOutputs;
    	this.sizeInMB = sizeInMB;
    	this.marker = marker;
    }
    
    public PersistedAction(Action action, ObjectId _id,
    		Long workflowId,
            ActionState state, Date lastUpdatedDate, 
            int version, String submissionId,
            Date startTime, Date endTime, List<String> parentsActionIds,
            List<String> parentActionOutputs,
            Double sizeInMB, Long marker) {
    	Preconditions.checkNotNull(action);
    	Preconditions.checkNotNull(_id);
    	Preconditions.checkNotNull(state);
    	Preconditions.checkNotNull(lastUpdatedDate);
    	this.action = action;
    	this._id = _id;
    	this.state = state;
    	this.lastUpdatedDate = lastUpdatedDate;
    	this.version = version;
    	this.submissionId = submissionId;
    	this.startTime = startTime;
    	this.workflowId = workflowId;
    	this.endTime = endTime;
    	this.parentsActionIds = parentsActionIds;
    	this.parentActionOutputs = parentActionOutputs;
    	this.sizeInMB = sizeInMB;
    	this.marker = marker;
    } 
    
    public Action getAction() {
        return action;
    }

    public ObjectId getId() {
        return _id;
    }

    public ActionState getState() {
        return state;
    }

    public Date getLastUpdatedDate() {
        return lastUpdatedDate;
    }
    
    void setVersion(int version) {
    	this.version = version;
    }

    public Long getWorkflowId() {
    	return this.workflowId;
    }
    
	public int getVersion() {
		return version;
	}

	public ObjectId get_id() {
		return _id;
	}
	
	public String getSubmissionId() {
		return this.submissionId;
	}
	
	public List<String> getParentActionIds() {
		return this.parentsActionIds;
	}
	
	public List<String> getParentActionOutputs() {
		return this.parentActionOutputs;
	}
	
	public Long getMarker() {
		return this.marker;
	}
	
	public Double getSizeInMB() {
		return this.sizeInMB;
	}
	
	public Date getStartTime() {
		return startTime;
	}
	
	public Date getEndTime() {
		return endTime;
	}

	public static PersistedAction parseAction(Document document) throws
    	WorkflowParseException, NullPointerException, JsonParseException {

		ObjectId id = document.getObjectId("_id");
		Date date = (Date) document.getDate("lastUpdatedDate");
		String stateString = document.getString("state");
		ActionState state = ActionState.valueOf(stateString);
		String submissionId = document.getString("submissionId");
		Long workflowId = document.getLong("workflowId");
		Date startTime = document.getDate("startTime");
		Date endTime = document.getDate("endTime");
		Double sizeInMB = document.getDouble("sizeInMB");
		Long marker = document.getLong("marker");
		Document actionDoc = (Document) document.get("action");
		Action action = PARSER.parseAction(actionDoc);
		int version = document.getInteger("version");
		List<String> parentsActionIds = (List<String>) document.get("parentsActionIds", List.class);
		List<String> parentActionOutputs = (List<String>) document.get("parentActionOutputs", List.class);
		
		return new PersistedAction(action, id, workflowId, state, date, version, submissionId,
				startTime, endTime, parentsActionIds, parentActionOutputs, sizeInMB, marker);
	}
}
