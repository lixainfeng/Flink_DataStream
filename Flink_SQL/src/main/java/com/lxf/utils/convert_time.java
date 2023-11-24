package com.lxf.utils;


public class convert_time {

    public static Long change_time(String time) {
        String [] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun","Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
        String [] months_num = {"1", "2", "3", "4", "5", "6","7", "8", "9", "10", "11", "12"};
        String s = time.split(" ")[0];
        String[] s1 = s.split("-");
        String mon = s1[1];
        String newdateString = "";
        for (int i = 0; i < 12; i++) {
            if (mon.equals(months[i])){
                mon = months_num[i];
                newdateString = s.replace(s1[0], "2021").replace(s1[1], mon);
                break;
            }
        }
        String replace_string = time.replace(s, newdateString);
        Long Long_time = StringTimeToTimeStamp.strtime_timestamp(replace_string);
        return Long_time;
    }
}
