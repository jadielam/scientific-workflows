package io.biblia.workflows.definition;

import io.biblia.workflows.definition.parser.ActionNameConstants;

/**
 * Created by dearj019 on 4/12/16.
 */
public enum ActionType implements ActionNameConstants {

    COMMAND_LINE(JAVA_ACTION),
    FS_DELETE(FS_DELETE_ACTION),
    MAP_REDUCE_1(MAP_REDUUCE_1_ACTION),
    MAP_REDUCE_2(MAP_REDUCE_2_ACTION);

    private final String type;
    ActionType(String type) {

        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    /**
     * Returns the enum type for an action.
     * It returns null if it cannot find it.
     * @param type
     * @return
     */
    public static ActionType fromString(String type) {
        if (type != null) {
            for (ActionType a : ActionType.values()) {
                if (type.equals(a.type)) {
                    return a;
                }
            }
        }
        return null;
    }

}
