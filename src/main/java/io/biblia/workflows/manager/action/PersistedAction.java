package io.biblia.workflows.manager.action;

import com.google.common.base.Preconditions;
import io.biblia.workflows.definition.Action;

import java.util.Date;

/**
 * Created by dearj019 on 4/12/16.
 */
public class PersistedAction {

    private final Action action;
    private final String id;
    private final ActionState state;
    private final Date lastUpdatedDate;

    public PersistedAction(Action action, String id,
                           ActionState state, Date lastUpdatedDate) {
        Preconditions.checkNotNull(action);
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(state);
        Preconditions.checkNotNull(lastUpdatedDate);
        this.action = action;
        this.id = id;
        this.state = state;
        this.lastUpdatedDate = lastUpdatedDate;
    }

    public Action getAction() {
        return action;
    }

    public String getId() {
        return id;
    }

    public ActionState getState() {
        return state;
    }

    public Date getLastUpdatedDate() {
        return lastUpdatedDate;
    }
}
