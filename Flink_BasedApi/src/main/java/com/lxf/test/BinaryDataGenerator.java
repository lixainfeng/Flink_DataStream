package com.lxf.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class BinaryDataGenerator {
    public static void main(String[] args) throws IOException {
        int dd = 102400; // 二进制数据的长度
        byte[] binaryData = new byte[dd];
        new Random().nextBytes(binaryData);

        // 将二进制数据写入到文件
        String filePath = "/Users/shuofeng/Desktop/file1.bin";
        FileOutputStream fos = new FileOutputStream(filePath);
        fos.write(binaryData);
        fos.close();
    }
}
