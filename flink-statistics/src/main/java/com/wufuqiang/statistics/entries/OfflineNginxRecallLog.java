package com.wufuqiang.statistics.entries;


import lombok.Data;

import java.io.Serializable;

@Data
public class OfflineNginxRecallLog implements Serializable {

    private String nRecallSceneId ;
    private String nRecallUserId ;

    public OfflineNginxRecallLog(){}

    public OfflineNginxRecallLog(String nRecallSceneId, String nRecallUserId) {
        this.nRecallSceneId = nRecallSceneId;
        this.nRecallUserId = nRecallUserId;
    }
}
