package com.wufuqiang.auc.entries;

import lombok.Data;

import java.io.Serializable;

@Data
public class RecallLog implements Serializable {
    private String sceneId ;
    private String userId;
    private String itemId;
    private String score ;
    private String recallScore;

    public RecallLog(){}

    public RecallLog(String sceneId, String userId, String itemId, String score, String recallScore) {
        this.sceneId = sceneId;
        this.userId = userId;
        this.itemId = itemId;
        this.score = score;
        this.recallScore = recallScore;
    }
}
