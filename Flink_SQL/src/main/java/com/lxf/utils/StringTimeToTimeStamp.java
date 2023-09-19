package com.lxf.utils;


import java.text.SimpleDateFormat;
import java.util.Date;

public class StringTimeToTimeStamp {
/*    public static void main(String[] args) {
        strtime_timestamp("2019-12-01 10:02:15");
    }*/

    public static Long strtime_timestamp(String times) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date = format.parse(times); // 将时间字符串转换为Date类型
            Long timestamp = date.getTime(); // 获取时间戳
            return timestamp;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
