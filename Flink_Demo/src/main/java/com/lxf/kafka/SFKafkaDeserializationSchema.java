package com.lxf.kafka;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;


public class SFKafkaDeserializationSchema implements KafkaDeserializationSchema<Tuple2<String,String>>{
    @Override
    public boolean isEndOfStream(Tuple2<String, String> stringStringTuple2) {
        return false;
    }

    @Override
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        String topic = consumerRecord.topic();
        int partition = consumerRecord.partition();
        long offset = consumerRecord.offset();
        String id = topic+"_"+partition+"_"+offset;
        String value = new String(consumerRecord.value(), StandardCharsets.UTF_8);

        return Tuple2.of(id, value);
    }

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});
    }
}
