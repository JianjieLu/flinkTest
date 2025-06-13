package whu.edu.moniData.Utils;

import java.math.BigInteger;

public class BytesToDecExample {

    public static void main(String[] args) {
        // 示例：4字节数组
        byte[] bytes = { (byte) 0xD1, (byte) 0xF3, (byte) 0x32, (byte) 0x20 };

        // 1. 字节数组转十六进制字符串
        String hexString = bytesToHexString(bytes);
        System.out.println("十六进制表示: 0x" + hexString);

        // 2. 十六进制字符串转十进制数字
        long decimalValue = hexToDecimal(hexString);
        System.out.println("十进制表示: " + decimalValue);

        // 更多示例
        testConversion(new byte[]{(byte) 0xFF}); // 单字节
        testConversion(new byte[]{(byte) 0xFF, (byte) 0xFF}); // 双字节
        testConversion(new byte[]{(byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00}); // 4字节
        testConversion(new byte[]{(byte) 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}); // 4字节
    }

    // 测试转换方法
    private static void testConversion(byte[] bytes) {
        String hex = bytesToHexString(bytes);
        long decimal = hexToDecimal(hex);

        System.out.println("\n字节数组: " + byteArrayToString(bytes));
        System.out.println("十六进制: 0x" + hex);
        System.out.println("十进制值: " + decimal);
    }

    // 1. 字节数组转十六进制字符串
    public static String bytesToHexString(byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            hex.append(String.format("%02X", b));
        }
        return hex.toString();
    }

    // 2. 十六进制字符串转十进制数字
    public static long hexToDecimal(String hexString) {
        // 对于长度超过8个字符（4字节）的十六进制字符串，使用BigInteger
        if (hexString.length() > 8) {
            BigInteger bigInt = new BigInteger(hexString, 16);
            return bigInt.longValue(); // 注意：大于8字节的值可能溢出
        }

        // 直接处理较短的值
        return Long.parseLong(hexString, 16);
    }

    // 辅助方法：格式化字节数组为可读字符串
    private static String byteArrayToString(byte[] bytes) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < bytes.length; i++) {
            sb.append(String.format("0x%02X", bytes[i]));
            if (i < bytes.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}