package com.lxf.sink;

import redis.clients.jedis.Jedis;

public class TestConnectRedis {
    public static void main(String[] args) {

        Jedis jedis = new Jedis("42.192.196.73",6379);
        //执行操作
        jedis.set("001", "flink");

        String s = jedis.get("001");
        System.out.println(s);

        //关闭连接
        jedis.close();
    }
}
