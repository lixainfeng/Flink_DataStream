package com.lxf.test;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

public class LastValueALLFilterNull extends AggregateFunction<String, Tuple3<String, List<String>, String>> {


    //初始化count UDAF的accumulator。
    @Override
    public Tuple3<String, List<String>, String> createAccumulator() {
        Tuple3<String, List<String>, String> acc = new Tuple3<>();
        acc.f0 = null;
        acc.f1 = new ArrayList<>();
        acc.f2 = "";
        return acc;
    }

    @Override
    public String getValue(Tuple3<String, List<String>, String> acc) {
        try {
            List<String> list = acc.f1;
            StringBuffer stringBuffer = new StringBuffer();
            for (String value : list) {
                stringBuffer.append(value).append(acc.f2);
            }
            return "".equals(stringBuffer.toString()) ? null : stringBuffer.toString();
        } catch (Exception e) {
        }
        return null;
    }

    /**
     * 累加方法
     * @param acc
     * @param key 排序依据字段
     * @param separator 输出拼接字符串里，字段之间的分隔符
     * @param values 多个字段
     */
    public void accumulate(Tuple3<String, List<String>, String> acc, String key, String separator, Object... values) {
        try {
            if (null != separator && null != key && null != values) {
                acc.f2 = separator;
                // 数据第一次来，直接将当前数据放入acc
                if (acc.f0 == null) {
                    acc.f0 = key;
                    for (Object value : values) {
                        acc.f1.add(String.valueOf(value));
                    }
                }
                //当前时间比之前的大，判断当前数据是否为null
                else if (acc.f0.compareTo(key) <= 0) {
                    acc.f0 = key;
                    int i = 0;
                    for (Object value : values) {
                        if (null != value) {
                            acc.f1.remove(i);
                            acc.f1.add(i, String.valueOf(value));
                        }
                        i++;
                    }
                }
            }
        } catch (Exception e) {
        }

    }

    public void retract(Tuple3<String, List<String>, String> acc, String key, String separator, Object... values) {
        return;
    }
}
