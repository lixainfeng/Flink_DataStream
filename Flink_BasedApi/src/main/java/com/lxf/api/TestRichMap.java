package com.lxf.api;

import com.lxf.model.Access;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 增强mapfunction方法
 */
public class TestRichMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textFile = env.readTextFile("/Users/shuofeng/IdeaProjects/Test/flink-datastream/src/main/java/com/lxf/data/1.txt");
        SingleOutputStreamOperator<Access> mapsource = textFile.map(new RichMapFun());
        env.setParallelism(2);//设置两个并行度，也就是两个task
        mapsource.print();
        env.execute();
    }
    public static  class RichMapFun extends RichMapFunction<String, Access> {
        /**
         * 初始化操作,每个task会打开一个open
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("----open----");
        }

        /**
         *关闭会话
         */
        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return super.getRuntimeContext();
        }

        /**
         * 处理业务逻辑,每一条数据都会进入map处理一次
         */
        @Override
        public Access map(String s) throws Exception {
            System.out.println("&&&&map&&&&&");
            String[] value = s.split(",");
            long time = Long.parseLong(value[0]);
            String name = value[1];
            Integer count = Integer.parseInt(value[2]);
            return new Access(time,name,count);
        }
    }
}
