package com.wufuqiang.itemcf.entries;

import lombok.Data;

@Data
public class PetalItem {
    private String userId;
    private String itemId;
    private long actionTime;
    private int flag;

    public PetalItem() {
    }

    public PetalItem(String userId, String itemId) {
        this.userId = userId;
        this.itemId = itemId;
    }

    public PetalItem(String userId, String itemId, long actionTime) {
        this.userId = userId;
        this.itemId = itemId;
        this.actionTime = actionTime;
    }

    public PetalItem(String userId, String itemId,long actionTime, int flag) {
        this.userId = userId;
        this.itemId = itemId;
        this.actionTime = actionTime;
        this.flag = flag;
    }


}
