package com.wufuqiang.auc.entries;


import lombok.Data;

import java.io.Serializable;

@Data
public class ActionLog implements Serializable {
    private String actionSceneId ;
    private String actionUserId;
    private String actionItemId;
    private String action ;
    private String recallStrategyId ;
    private String channel ;
    private String sortStrategyId ;
    private String predictModelId ;
    private Integer label;

    public ActionLog(){}

    public ActionLog(String actionSceneId, String actionUserId, String actionItemId, String action, String recallStrategyId, String channel, String sortStrategyId, String predictModelId, Integer label) {
        this.actionSceneId = actionSceneId;
        this.actionUserId = actionUserId;
        this.actionItemId = actionItemId;
        this.action = action;
        this.recallStrategyId = recallStrategyId;
        this.channel = channel;
        this.sortStrategyId = sortStrategyId;
        this.predictModelId = predictModelId;
        this.label = label;
    }
}