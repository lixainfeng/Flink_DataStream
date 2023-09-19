package com.lxf.udf;


import com.lxf.utils.IPpaserUtil;
import org.apache.flink.table.functions.ScalarFunction;

public class UdfWithIpPaser extends ScalarFunction{

    public String eval(String ip){
        return IPpaserUtil.getAddress(ip);
    }
}

