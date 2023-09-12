package com.lxf.dataset;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * 分布式缓存适用于大表join小表
 * 小表加载至缓存
 */
public class DistributedCacheApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.fromElements("spark,flink,flink");
        List<String> list = new ArrayList<>();
        //注册至分布式缓存中
        env.registerCachedFile("/Users/shuofeng/IdeaProjects/Test/Flink_Project/Flink_DataSet/src/main/java/com/lxf/data/a/1.txt","localfile");
        data.map(new RichMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                //通过上下文获取到缓存中的文件
                File file = getRuntimeContext().getDistributedCache().getFile("localfile");
                List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
                for (String line : lines) {
                   list.add(line);
                    System.out.println("line====>"+line);
                }
                super.open(parameters);
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
        env.execute();
    }
}
