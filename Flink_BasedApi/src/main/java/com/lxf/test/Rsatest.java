package com.lxf.test;

public class Rsatest {
    public static void main(String[] args) throws Exception {

        /**
         * 第一步：对请求body体进行加密
         */
        // post方式,body请求体内容，这里是指生成API里的输入参数
        String content = "{\"inFields\":{\"id\":1}}";

        //1。 随机生成AES秘钥，用来对body进行加密
        String aesKeyStr = AESUtil.generateKey(128);
        System.out.println("生成出来的aesKey:"+aesKeyStr);

        //2。 使用生成的AES密钥，对请求内容进行加密
        String data = AESUtil.encrypt(content, aesKeyStr);
        System.out.println("请求中的data数据密文:"+data);

        //3。 通过RSA公钥对base64编码后的AES秘钥进行加密（这个公钥在平台API管理里的API调用）
        String publicKey = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCKRkg9T8Rt/LmFmgOsdpZvX4rNhuNgWx42TcKZ54QGAS4CJ77DHbO09g7/WCyO0ecDFCwWf5fUwllSJse453JAXhqHbttG257Gd7C0g6VPPifOipd/4gDlkXyruPnBS7T9FFbz4eNvZyTn9cHEGn23KEJzLyOWmmDGWQ2Bn1IHlwIDAQAB";
        String aesKey = RSAUtil.encrypt(aesKeyStr, publicKey);
        System.out.println("请求中的aesKey密文:"+aesKey);





        /**
         * 第二步：对请求返回值data进行解密
         *这个data值就是我们最终要获取的结果集，只是被加密了
         *
         */
        //1。将API请求后的返回结果中data值复制出来
        StringBuilder resultData = new StringBuilder();
        resultData.append("RW0/4hhn/qfub4uJtOtC2luy45oWfVrqgkPBZpeg0SlX7pRqD7yXiJceRElWRgs+e9l5xnRIKsj8fqM30VXBfLEFQpfko0QJhBhC/FojS9XYdCT9/vM0NIM1JAhoNPtk2RLyEresseM62BlIdfHpbg1/5mz7QHq6jXo72fZFlUSsPw8cBKQEIhU66rmalUJO" );
        //2。使用第一步生成的aes密钥对结果数据进行解密
        aesKey = "EVgDm1f/CVWKMqoREaoHSw==";
        content = AESUtil.decrypt(resultData.toString(), aesKey);
        System.out.println("请求返回结果解密后的数据明文:"+content);
    }
}
