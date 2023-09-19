package com.lxf.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UdtfWithFiledPaser extends TableFunction<Row> {

    /**
     * 自动类型推导输入参数类型和输出结果类型->DataTypeHint
     * @param value
     */
    @DataTypeHint("ROW<s String,t String>")
    public void eval(String value){
        String[] split = value.split(" ");
        Row row = new Row(2);
        row.setField(0,split[0]);
        row.setField(1,split[1]);
        collect(row);
    }
}
