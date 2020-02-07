package com.wufuqiang.statistics.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtils {

    public static String getYesterday(String today) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date todayDay = sdf.parse(today);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(todayDay);
        calendar.add(Calendar.DAY_OF_MONTH,-1);
        Date yesterdayDay = calendar.getTime();
        String yesterday = sdf.format(yesterdayDay);
        return yesterday;
    }

    public static String[] getLastHour(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd-HH");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.HOUR_OF_DAY,-1);
        String lasthour = sdf.format(calendar.getTime());
        return lasthour.split("-");

    }

}
