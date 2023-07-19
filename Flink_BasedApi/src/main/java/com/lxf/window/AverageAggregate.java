package com.lxf.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 *
 <IN> – 输入值
 <ACC> – 中间聚合的值
 <OUT> – 输出结果的值
 */
public class AverageAggregate implements AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double> {
    @Override
    public Tuple2<Long, Long> createAccumulator() { //创建一个累加器，并初始化累加器的值
        return new Tuple2<>(0L, 0L);
    }


    @Override
    public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
        //System.out.println("累加器的值和传入初始值进行累加:"+accumulator.f0 + value.f1+"累加器初始值加一:"+accumulator.f1 + 1L);
        return new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1L);//累加器的值和传入初始值进行累加,累加器初始值加一
    }

    @Override
    public Double getResult(Tuple2<Long, Long> accumulator) {
        //System.out.println("分子："+accumulator.f0+"分母："+accumulator.f1);
        return ((double) accumulator.f0) / accumulator.f1;
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);//相同key的数据进行合并
    }
}
