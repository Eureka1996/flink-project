package com.wufuqiang.auc.entries;


import lombok.Data;

import java.io.Serializable;

@Data
public class ItemWithRank implements Serializable, Comparable<ItemWithRank> {
    private String sceneId;
    private String userId ;
    private String channel;
    private Integer label;
    private Double score;
    private Double rank;

    public ItemWithRank(){}

    public ItemWithRank(String sceneId,String userId, String channel, Integer label, Double score,Double rank) {
        this.sceneId = sceneId;
        this.userId = userId;
        this.channel = channel;
        this.label = label;
        this.score = score;
        this.rank = rank;
    }

    @Override
    public int compareTo(ItemWithRank other) {
        return this.getScore().compareTo(other.getScore());
    }
}
