package whu.edu.moniData;//package whu.edu.moniData;
//
//import java.util.Arrays;
//
//public class test {
//    public static void main(String[] args) {
//        byte[] data = new byte[] {
//                (byte) 0xFF, (byte) 0xFF, (byte) 0x00, (byte) 0xE1, (byte) 0x05, (byte) 0x00,
//                (byte) 0x01, (byte) 0x98, (byte) 0x08, (byte) 0x08, (byte) 0x10, (byte) 0xB4,
//                (byte) 0xF3, (byte) 0x05, (byte) 0x1A, (byte) 0x12, (byte) 0x08, (byte) 0x02,
//                (byte) 0x10, (byte) 0x01, (byte) 0x18, (byte) 0xDF, (byte) 0xFC, (byte) 0xD3,
//                (byte) 0x09, (byte) 0x12, (byte) 0x08, (byte) 0x01, (byte) 0x10, (byte) 0x01,
//                (byte) 0x18, (byte) 0xDF, (byte) 0xFC, (byte) 0xD3, (byte) 0x9D, (byte) 0xF4,
//                (byte) 0x32, (byte) 0x20, (byte) 0xDF, (byte) 0xFC, (byte) 0xD3, (byte) 0x9D,
//                (byte) 0xF4, (byte) 0x32, (byte) 0x1A, (byte) 0x04, (byte) 0x08, (byte) 0x01,
//                (byte) 0x10, (byte) 0x02, (byte) 0x02, (byte) 0x30, (byte) 0x0B, (byte) 0x38,
//                (byte) 0x62, (byte) 0x40, (byte) 0x0B, (byte) 0x58, (byte) 0x02, (byte) 0x60,
//                (byte) 0xE7, (byte) 0x9D, (byte) 0xE3, (byte) 0x9F, (byte) 0x04, (byte) 0x68,
//                (byte) 0xF4, (byte) 0xB0, (byte) 0xB7, (byte) 0x93, (byte) 0x01, (byte) 0x78,
//                (byte) 0xC8, (byte) 0xDC, (byte) 0x03, (byte) 0x80, (byte) 0x01, (byte) 0xFF,
//                (byte) 0xFF, (byte) 0x01, (byte) 0xB8, (byte) 0x01, (byte) 0xFA, (byte) 0x48,
//                (byte) 0x4A, (byte) 0x4E, (byte) 0x10, (byte) 0xC9, (byte) 0x24, (byte) 0x20,
//                (byte) 0x0A, (byte) 0x28, (byte) 0x62, (byte) 0x30, (byte) 0x0A, (byte) 0x38,
//                (byte) 0x62, (byte) 0x40, (byte) 0x0A, (byte) 0x58, (byte) 0x01, (byte) 0x60,
//                (byte) 0x8B, (byte) 0x0E, (byte) 0x81, (byte) 0xFA, (byte) 0xFF, (byte) 0xFF,
//                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x01,
//                (byte) 0xA8, (byte) 0x01, (byte) 0xAF, (byte) 0x04, (byte) 0xB0, (byte) 0x01,
//                (byte) 0xBC, (byte) 0xFD, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
//                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x01, (byte) 0x07, (byte) 0x93,
//                (byte) 0x01, (byte) 0x78, (byte) 0xE8, (byte) 0x07, (byte) 0x80, (byte) 0x01,
//                (byte) 0x04, (byte) 0x88, (byte) 0x01, (byte) 0xB5, (byte) 0x03, (byte) 0x90,
//                (byte) 0x01, (byte) 0xB6, (byte) 0x01, (byte) 0x98, (byte) 0x01, (byte) 0x9D,
//                (byte) 0x01, (byte) 0xA0, (byte) 0x01, (byte) 0xFE, (byte) 0xF3, (byte) 0xFF,
//                (byte) 0xFF, (byte) 0x20, (byte) 0x01, (byte) 0x28, (byte) 0x63, (byte) 0x30,
//                (byte) 0x01, (byte) 0x38, (byte) 0x63, (byte) 0x40, (byte) 0x01, (byte) 0x58,
//                (byte) 0x01, (byte) 0x60, (byte) 0xA4, (byte) 0xE3, (byte) 0xE2, (byte) 0x9F,
//                (byte) 0x04, (byte) 0x68, (byte) 0xEB, (byte) 0xC6, (byte) 0xB7, (byte) 0x93,
//                (byte) 0x01, (byte) 0x78, (byte) 0xF8, (byte) 0x0A, (byte) 0x01, (byte) 0xBF,
//                (byte) 0x07, (byte) 0xB0, (byte) 0x01, (byte) 0xEF, (byte) 0xFC, (byte) 0xFF,
//                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
//                (byte) 0x01, (byte) 0xB8, (byte) 0x01, (byte) 0x2E, (byte) 0x60, (byte) 0xC3,
//                (byte) 0x01, (byte) 0xC0, (byte) 0xFF
//        };
//
//        // 输出数组长度
//        System.out.println("原始字节数组长度: " + data.length);
//        System.out.println("将输出 " + (data.length - 3) + " 个4字节组合\n");
//
//        // 每4个字节组成一个整数并输出
//        for (int i = 0; i < data.length - 3; i++) {
//            // 获取4字节片段
//            byte[] chunk = Arrays.copyOfRange(data, i, i + 4);
//
//            // 计算整数值（大端序）
//            int value = ((chunk[0] & 0xFF) << 24) |
//                    ((chunk[1] & 0xFF) << 16) |
//                    ((chunk[2] & 0xFF) <<  8) |
//                    (chunk[3] & 0xFF);
//
//            // 格式化的十六进制表示
//            String hex = String.format("0x%02X%02X%02X%02X",
//                    chunk[0] & 0xFF, chunk[1] & 0xFF, chunk[2] & 0xFF, chunk[3] & 0xFF);
//
//            // 输出结果 (带位置索引)
//            System.out.printf("位置[%3d-%3d]: 字节 %s -> 整数 %,15d (0x%08X)%n",
//                    i, i + 3, hex, value, value);
//        }
//    }
//}

public class test {
    public static void main(String[] args) {
        System.out.println(20971520+629145600);
    }
}