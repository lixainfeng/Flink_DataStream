package com.lxf.udf;


import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

public  class UdafWithSum extends AggregateFunction<Double, UdafWithSum.AvgAccum> {
    @Override
    public Double getValue(AvgAccum accum) {
        if (accum.counts == 0) {
            return null;
        }else{
            return accum.sumprice * 1D / accum.counts;
        }
    }

    @Override
    public AvgAccum createAccumulator() {
        AvgAccum accum = new AvgAccum();
        accum.sumprice = 0D;
        accum.counts = 0;
        return accum;
    }

    public static class AvgAccum{
        public Double sumprice;
        public Integer counts;
    }


 /*   @FunctionHint(
            //accumulator = @DataTypeHint(bridgedTo = AvgAccum.class),
            input = @DataTypeHint("Integer"),
            output = @DataTypeHint("Double")
    )*/

    /**
     * 实现累加器
     * @param accum 聚合结果
     * @param v1 用户定义的输入值
     */
    public void accumulate(AvgAccum accum,Double v1){
        accum.sumprice += v1;
        accum.counts += 1;
    }

    // 在 bounded OVER 窗口中是必须实现的
    public void retract(AvgAccum accum,Double v1){
        accum.sumprice -= v1;
        accum.counts -= 1;
    }

    //在批式聚合和会话以及滚动窗口聚合中是必须实现的
    public void merge(AvgAccum accum,Iterable<AvgAccum> it){
        Iterator<AvgAccum> iter = it.iterator();
        while (iter.hasNext()) {
            AvgAccum a = iter.next();
            accum.sumprice += accum.sumprice;
            accum.counts += accum.counts;
        }
    }
}

