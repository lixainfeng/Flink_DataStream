package com.lxf.fink.cdc;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * 自定义反序列化器
 */
public class CustomDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        /**
         * Struct{
         * 	after=Struct{id=6,name=z},
         * 	source=Struct{version=1.4.1.Final,connector=mysql,name=mysql_binlog_source,ts_ms=0,snapshot=true,db=test,table=test4,server_id=0,file=mysql-bin.000004,pos=4329,row=0},
         * 	op=c
         * }
         */

        //before
        Struct value = (Struct) sourceRecord.value();//通过结构体获取字段数据，注意；这里不是json
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");
        Struct source = value.getStruct("source");
        String db = source.getString("db");
        String table = source.getString("table");



        //c=>create u=>update r=>read d=>delete
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String optype = operation.toString().toLowerCase();//根据官方接口获取op这个字段并转换成相应的操作

        JSONObject json = new JSONObject();
        JSONObject afterjson = new JSONObject();
        JSONObject beforejson = new JSONObject();

        //before
        if(before != null){
            List<Field> fields = before.schema().fields();//获取结构体
            for (Field field : fields) {
                beforejson.put(field.name(),before.get(field));
                if(before.get(field).toString().equals("ss")){
                    throw new RuntimeException("ERROR....");
                }else {
                    continue;
                }
            }
        }

        //after
        if(after != null){
            List<Field> fields = after.schema().fields();//获取结构体
            for (Field field : fields) {
                afterjson.put(field.name(),after.get(field));
                if(after.get(field).toString().equals("ss")){
                    throw new RuntimeException("ERROR....");
                }else {
                    continue;
                }
            }
        }

        json.put("db",db);
        json.put("table",table);
        json.put("before",beforejson);
        json.put("after",afterjson);
        json.put("op",optype);



        collector.collect(json.toString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return  BasicTypeInfo.STRING_TYPE_INFO;
    }
}
