package com.lxf.test;

import org.apache.commons.lang3.time.FastDateFormat;

public class sftest {
    public static void main(String[] args) {
        FastDateFormat instances = FastDateFormat.getInstance("yyyyMMdd-HH");
        long time = Long.parseLong("1801085116692");
        String value = instances.format(time);
        System.out.println(value);
    }
}
