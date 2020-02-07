package com.wufuqiang.flink.entries;

import com.alibaba.fastjson.JSONObject;
import com.wufuqiang.flink.common.Constants;
import lombok.Data;
import lombok.Getter;

@Data
public class ActionLog {
    @Getter
    private String sceneId ;
    private String action ;
    private long logWriteTime;

    public ActionLog() {
    }

    public ActionLog(String sceneId, String action, long logWriteTime) {
        this.sceneId = sceneId;
        this.action = action;
        this.logWriteTime = logWriteTime;
    }

    public static ActionLog getInstance(String json){
        JSONObject jsonObject = null;
        try{
            jsonObject = JSONObject.parseObject(json);
        }catch (Exception e){
            return null;
        }
        if(jsonObject == null){
            return null;
        }

        try{
            String sceneId = jsonObject.getString("sceneId");
            if(!Constants.whiteList.contains(sceneId)){
                return null;
            }
            String action = jsonObject.getString("action");
            long logWriteTime = jsonObject.get("logWriteTime") == null?0:(long)Double.parseDouble(jsonObject.get("logWriteTime").toString());
            return new ActionLog(sceneId,action,logWriteTime);
        }catch(Exception e){

        }

        return null;
    }
}
