package com.wufuqiang.flink.entries;

import lombok.Data;

@Data
public class MysqlItem {
    private String id;
    private String itemId;
    private String title;
    private String tag;
    private String content;

    public MysqlItem() {
    }

    public MysqlItem(String id, String itemId, String title, String tag, String content) {
        this.id = id;
        this.itemId = itemId;
        this.title = title;
        this.tag = tag;
        this.content = content;
    }

    @Override
    public String toString() {
        return "MysqlItem{" +
                "id='" + id + '\'' +
                ", itemId='" + itemId + '\'' +
                ", title='" + title + '\'' +
                ", tag='" + tag + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
