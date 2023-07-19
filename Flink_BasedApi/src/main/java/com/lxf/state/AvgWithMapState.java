package com.lxf.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.UUID;

public class AvgWithMapState extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Double>> {
    private transient MapState<String,Long> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<String, Long>("avg", String.class, Long.class);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
        mapState.put(UUID.randomUUID().toString(), value.f1);

        ArrayList<Long> elements = Lists.newArrayList(mapState.values());

        if(elements.size() == 3) {
            Long count = 0L;
            Long sum = 0L;

            for (Long element : elements) {
                count += 1;
                sum += element;
            }

            Double avg = sum / count.doubleValue();
            out.collect(Tuple2.of(value.f0, avg));

            mapState.clear();
        }

    }
}
