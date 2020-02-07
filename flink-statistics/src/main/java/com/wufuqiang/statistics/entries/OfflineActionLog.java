package com.wufuqiang.statistics.entries;

import lombok.Data;

import java.io.Serializable;

@Data
public class OfflineActionLog implements Serializable {

    private String actionSceneId;
    private String actionUserId;
    private String actionItemId;
    private String action;

    public OfflineActionLog(){}

    public OfflineActionLog(String actionSceneId, String userId, String itemId, String action) {
        this.actionSceneId = actionSceneId;
        this.actionUserId = userId;
        this.actionItemId = itemId;
        this.action = action;
    }
}
