package com.wufuqiang.auc.entries;


import lombok.Data;

import java.io.Serializable;

@Data
public class LabelScoreItem implements Serializable {
    private String sceneId;
    private String userId;
    private String recallStrategyId;
    private String channel;
    private String sortStrategyId;
    private String predictModelId;
    private Integer label;
    private String score;
    private String recallScore;

    public LabelScoreItem(){}

    public LabelScoreItem(String sceneId, String userId,String recallStrategyId, String channel, String sortStrategyId, String predictModelId, Integer label, String score,String recallScore) {
        this.sceneId = sceneId;
        this.userId = userId;
        this.recallStrategyId = recallStrategyId;
        this.channel = channel;
        this.sortStrategyId = sortStrategyId;
        this.predictModelId = predictModelId;
        this.label = label;
        this.score = score;
        this.recallScore = recallScore;
    }
}
