package com.wufuqiang.auc.entries;


import lombok.Data;

import java.io.Serializable;

@Data
public class UaucItemWithRank extends ItemWithRank implements Serializable {
    private Long show ;
    public UaucItemWithRank(){}

    public UaucItemWithRank(Long show) {
        this.show = show;
    }

    public UaucItemWithRank(String sceneId, String userId, String channel, Integer label, Double score, Double rank, Long show) {
        super(sceneId, userId, channel, label, score, rank);
        this.show = show;
    }
}
