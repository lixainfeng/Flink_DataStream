package com.lxf.test;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class AESUtil {
    public static void main(String[] args) throws Exception {
        // 生成AES秘钥
        String keyStr = generateKey(128);
        System.out.println("base64编码的aes秘钥:"+keyStr);

        // 加密
        String content = "{\"id\":1,\"name\":\"张三\"}";
        String encryptStr = encrypt(content, keyStr);
        System.out.println("base64编码的aes密文:"+encryptStr);

        // 解密
        String decrypt = decrypt(encryptStr, keyStr);
        System.out.println("解密后明文:" + decrypt);

    }

    /**
     * 生成AES秘钥
     * @param length
     * @return
     * @throws NoSuchAlgorithmException
     */
    public static String generateKey(int length) throws NoSuchAlgorithmException {
        KeyGenerator kgen = KeyGenerator.getInstance("AES");
        //设置密钥长度
        kgen.init(128);
        //生成密钥
        SecretKey skey = kgen.generateKey();
        //返回密钥的二进制编码
        return Base64.getEncoder().encodeToString(skey.getEncoded());
    }

    /**
     * 加密
     * @param content
     * @param keyStr
     * @return
     * @throws Exception
     */
    public static String encrypt(String content, String keyStr) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        byte[] keyBytes = Base64.getDecoder().decode(keyStr);
        SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");

        cipher.init(Cipher.ENCRYPT_MODE, key);
        //将加密并编码后的内容解码成字节数组
        return Base64.getEncoder().encodeToString(cipher.doFinal(content.getBytes()));
    }

    public static String decrypt(String encodeStr, String keyStr) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        byte[] keyBytes = Base64.getDecoder().decode(keyStr);
        byte[] encodeBytes = Base64.getDecoder().decode(encodeStr);
        SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");
        //初始化密码器，第一个参数为加密(Encrypt_mode)或者解密(Decrypt_mode)操作，第二个参数为使用的KEY
        cipher.init(Cipher.DECRYPT_MODE, key);
        return new String(cipher.doFinal(encodeBytes));
    }
}
