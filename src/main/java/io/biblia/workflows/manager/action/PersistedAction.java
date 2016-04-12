package io.biblia.workflows.manager.action;

import io.biblia.workflows.definition.Action;

/**
 * Created by dearj019 on 4/12/16.
 */
public class PersistedAction {

    private final Action action;
    private final String id;
    private final ActionState state;

    public PersistedAction(Action action, String id,
                           ActionState state) {
        this.action = action;
        this.id = id;
        this.state = state;
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
}
