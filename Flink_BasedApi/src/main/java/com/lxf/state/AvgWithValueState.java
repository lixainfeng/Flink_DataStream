package com.lxf.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class AvgWithValueState extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Double>> {
    // 求平均数：记录条数  总和
    private transient ValueState<Tuple2<Long,Long>> sum;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Tuple2<Long,Long>> avg = new ValueStateDescriptor<>("avg", Types.TUPLE(Types.LONG, Types.LONG));
        sum = getRuntimeContext().getState(avg);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> collector) throws Exception {
        // TODO... ==> state   次数 和 总和
        Tuple2<Long, Long> currentState = sum.value();
        if(null == currentState) {
            currentState = Tuple2.of(0L,0L);
        }

        currentState.f0 += 1; // 次数
        currentState.f1 += value.f1; // 求和


        sum.update(currentState);

        // 达到3条数据 ==> 求平均数  clear
        if(currentState.f0 >=3 ){
            collector.collect(Tuple2.of(value.f0, currentState.f1/currentState.f0.doubleValue()));
            sum.clear();
        }

    }
}
