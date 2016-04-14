package io.biblia.workflows.manager.action;

import com.google.common.base.Preconditions;
import io.biblia.workflows.definition.Action;
import org.bson.types.ObjectId;

import java.util.Date;

/**
 * Created by dearj019 on 4/12/16.
 */
public class PersistedAction {

    private final Action action;
    private final ObjectId _id;
    private final ActionState state;
    private final Date lastUpdatedDate;
    private int version;

    public PersistedAction(Action action, ObjectId _id,
                           ActionState state, Date lastUpdatedDate, 
                           int version) {
        Preconditions.checkNotNull(action);
        Preconditions.checkNotNull(_id);
        Preconditions.checkNotNull(state);
        Preconditions.checkNotNull(lastUpdatedDate);
        this.action = action;
        this._id = _id;
        this.state = state;
        this.lastUpdatedDate = lastUpdatedDate;
        this.version = version;
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

	public int getVersion() {
		return version;
	}

	public ObjectId get_id() {
		return _id;
	}
    
    
}
