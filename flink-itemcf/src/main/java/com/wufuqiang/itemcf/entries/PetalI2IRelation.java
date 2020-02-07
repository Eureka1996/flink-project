package com.wufuqiang.itemcf.entries;

import lombok.Data;

@Data
public class PetalI2IRelation {
    private String itemIdOne;
    private String itemIdOther;
    private double appearValue;
    private long userCountOne;
    private long userCountOther;
    private int flag;
    private double score;

    public PetalI2IRelation() {
    }

    public PetalI2IRelation(String itemIdOne, String itemIdOther, double appearValue, int flag) {
        this.itemIdOne = itemIdOne;
        this.itemIdOther = itemIdOther;
        this.appearValue = appearValue;
        this.flag = flag;
    }

    public PetalI2IRelation(String itemIdOne, String itemIdOther, double appearValue, long userCountOne, long userCountOther, int flag) {
        this.itemIdOne = itemIdOne;
        this.itemIdOther = itemIdOther;
        this.appearValue = appearValue;
        this.userCountOne = userCountOne;
        this.userCountOther = userCountOther;
        this.flag = flag;
    }
}
