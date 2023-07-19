package com.lxf.partition;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * 自定义分区器
 */
public class TestPartition implements Partitioner<String> {

    @Override
    public int partition(String s, int i) {
        if("zhangsan".equals(s)){
            return 0;
        }else if("lisi".equals(s)){
            return 1;
        }else{
            return 2;
        }

    }
}
