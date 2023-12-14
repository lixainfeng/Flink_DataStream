package com.lxf.Join;

import com.lxf.model.Item;
import com.lxf.model.Order;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 完整数据链路：
 * mysql --> canal --> kafka --> flink --> clickhouse
 */
public class FlinkjoinApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 9876);
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9875);

        SingleOutputStreamOperator<Order> OrderStream = source1.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                Order order = new Order();
                String[] split = value.split(",");
                order.setOrderId(split[0]);
                order.setTime(Long.parseLong(split[1]));
                order.setMoney(Double.parseDouble(split[2]));
                return order;
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                            @Override
                            public long extractTimestamp(Order element, long recordTimestamp) {
                                return element.getTime();
                            }
                        })
        );

        SingleOutputStreamOperator<Item> ItmeStream = source2.map(new MapFunction<String, Item>() {
            @Override
            public Item map(String value) throws Exception {
                Item item = new Item();
                String[] split = value.split(",");
                item.setItemId(split[0]);
                item.setOrderId(split[1]);
                item.setI_time(Long.parseLong(split[2]));
                item.setSuk(split[3]);
                item.setAmnout(Integer.parseInt(split[4]));
                item.setPrice(Double.parseDouble(split[5]));
                return item;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Item>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<Item>() {
                    @Override
                    public long extractTimestamp(Item element, long recordTimestamp) {
                        return element.getI_time();
                    }
                })
        );


        ItmeStream.print("Item--");
        OrderStream.print("Order--");
        //TODO 双流join ...
        /**
         * 分别有三种流流join算子
         * join:即为window join。按照指定的字段和窗口进行inner join
         * coGroup:实现left/right outer join,也可以指定窗口计算
         * Interval:时区间join。左/右流关联右/左流一定时间范围内的数据。它作用于keyedstream里，只能使用事件事件。
         *          stream1 s1 inner join stream2 s2 on s1.id=s2.id and s2.date between start_time ane end_time;
         */
        /*ItmeStream.join(OrderStream)
                .where(x -> x.getOrderId())
                .equalTo(y -> y.getOrderId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Item, Order, String>() {
                    @Override
                    public String join(Item first, Order second) throws Exception {
                        String itemId = first.getItemId();
                        String orderId = first.getOrderId();
                        Long i_time = first.getI_time();
                        String suk = first.getSuk();
                        Double money = second.getMoney();
                        return itemId + ","+ orderId+ ","+i_time + ","+ suk+ ","+money;
                    }
                }).print("join--");*/
        ItmeStream.coGroup(OrderStream)
                .where(x -> x.getOrderId())
                .equalTo(y -> y.getOrderId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Item, Order, Tuple2<Item,Order>>() {
                    @Override
                    public void coGroup(Iterable<Item> first, Iterable<Order> second, Collector<Tuple2<Item, Order>> out) throws Exception {
                        for (Item item : first) {
                            boolean flag = false;
                            for (Order order : second) {
                                out.collect(Tuple2.of(item,order));
                                flag = true;
                            }
                            if(!flag){
                                out.collect(Tuple2.of(item,null));
                            }
                        }
                    }
                }).print("join--");
        env.execute();
    }
}
