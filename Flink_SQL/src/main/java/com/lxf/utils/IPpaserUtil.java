package com.lxf.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;

public class IPpaserUtil implements Serializable {

    /**
     * 通过调用接口根据ip获取归属地
     */
    public static String getAddress(String ip) {
        try {
            URL realUrl = new URL("http://whois.pconline.com.cn/ipJson.jsp?ip=" + ip + "&json=true");
            HttpURLConnection conn = (HttpURLConnection) realUrl.openConnection();
            conn.setRequestMethod("GET");
            conn.setUseCaches(false);
            conn.setReadTimeout(6000);
            conn.setConnectTimeout(6000);
            conn.setInstanceFollowRedirects(false);
            int code = conn.getResponseCode();
            StringBuilder sb = new StringBuilder();
            String addr = "";
            if (code == 200) {
                InputStream in = conn.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(in, "GBK"));//指定编码格式
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                //{"ip":"61.175.192.50","pro":"浙江省","proCode":"330000","city":"杭州市","cityCode":"330100","region":"","regionCode":"0","addr":"浙江省杭州市 电信","regionNames":"","err":""}
                JSONObject jsonObject = JSON.parseObject(String.valueOf(sb));
                String pro = jsonObject.getString("pro".trim());
                String city = jsonObject.getString("city".trim());
                String s = pro+"-"+city;
                addr = s;

            }
            return addr;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
