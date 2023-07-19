package com.lxf.sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.collect.CollectSinkFunction;

public class TestCollectSinkFun extends CollectSinkFunction {
    public TestCollectSinkFun(TypeSerializer serializer, long maxBytesPerBatch, String accumulatorName) {
        super(serializer, maxBytesPerBatch, accumulatorName);
    }
}
