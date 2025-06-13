package whu.edu.moniData.Utils;

import java.util.Arrays;

public class HexStringConverter {
    public static void main(String[] args) {
        // 您提供的十六进制字符串
        String hexString = "FF FF 00 E1 02 00 00 46 00 0D 00 00 00 00 00 00 68 42 96 0A 00 09 89 68 12 6D A6 3A 12 6D A6 3A 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 53 FF";
        String[] hexPairs = hexString.split("\\s+");

        // 2. 创建对应的字节数组
        byte[] byteArray = new byte[hexPairs.length];

        // 3. 统计和转换
        System.out.println("十六进制字符对数量: " + hexPairs.length);
        System.out.println("字节数组长度: " + byteArray.length);
        System.out.println("\n字节对详情:");


        // 4. 输出完整字节数组
        System.out.println("\n完整字节数组: ");
        System.out.println(Arrays.toString(byteArray));

        // 5. 特别输出协议关键字段
        System.out.println("\n协议关键字段分析:");
        System.out.println("帧头: " + hexPairs[0] + " " + hexPairs[1] +
                " -> " + (byteArray[0] & 0xFF) + "," + (byteArray[1] & 0xFF));
        System.out.println("序列号: " + hexPairs[2] + " -> " + (byteArray[2] & 0xFF));
        System.out.println("主命令: " + hexPairs[3] + " -> 0x" + Integer.toHexString(byteArray[3] & 0xFF));
        System.out.println("子命令: " + hexPairs[4] + " -> " + (byteArray[4] & 0xFF));
        System.out.println("状态: " + hexPairs[5] + " -> " + (byteArray[5] & 0xFF));
        System.out.println("消息长度: " + hexPairs[6] + " " + hexPairs[7] +
                " -> " + ((byteArray[6] & 0xFF) << 8 | (byteArray[7] & 0xFF)));

        // 6. 完整消息内容长度分析
        int messageLength = ((byteArray[6] & 0xFF) << 8) | (byteArray[7] & 0xFF);
        System.out.println("\n完整数据包所需长度分析:");
        System.out.println("协议头长度: 8 字节");
        System.out.println("消息内容长度: " + messageLength + " 字节");
        System.out.println("校验位 + 结束标志: 2 字节");
        System.out.println("总长度需求: " + (8 + messageLength + 2) + " 字节");
        System.out.println("实际字符对数量: " + hexPairs.length + " 字节");
    }
}