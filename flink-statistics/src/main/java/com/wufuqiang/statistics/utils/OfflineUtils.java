package com.wufuqiang.statistics.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OfflineUtils {

    public static String[] getDayHour(String operationType,String day,String hour) throws ParseException {

        if("day".equals(operationType)){
            if("yesterday".equals(day)){
                Date current = new Date();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                String now = sdf.format(current);
                day = TimeUtils.getYesterday(now);
            }
            hour = "00";
        }

        if("hour".equals(operationType)){
            if("lasthour".equals(hour)){
                String[] lastHour = TimeUtils.getLastHour();
                day = lastHour[0];
                hour = lastHour[1];
            }
        }

        return new String[]{day,hour};
    }

    public static String getFilePath(String pathPrefis,String operationType,String day,String hour){
        String filePath = "";
        if("day".equals(operationType)){
            filePath = String.format("%s/%s",pathPrefis,day);
        } else if("hour".equals(operationType)){
            filePath = String.format("%s/%s/%02d",pathPrefis,day,Integer.parseInt(hour));
        }
        return filePath;
    }

    public static String getFilePathSpark(String pathPrefis,String operationType,String day,String hour){
        String filePath = "";
        if("day".equals(operationType)){
            filePath = String.format("%s/%s/*",pathPrefis,day);
        }else if("hour".equals(operationType)){
            filePath = String.format("%s/%s/%02d",pathPrefis,day,Integer.parseInt(hour));
        }
        return filePath;
    }

}
