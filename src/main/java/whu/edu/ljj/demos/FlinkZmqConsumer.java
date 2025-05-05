//package whu.edu.ljj.demos;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import whu.edu.ljj.demos.ZmqSource;
//
//import java.nio.charset.StandardCharsets;
//import java.util.Arrays;
//
//public class FlinkZmqConsumer {
//    public static void main(String[] args) throws Exception {
//        // 创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 设置数据源并处理数据
//        env.addSource(new ZmqSource())
//           .name("ZeroMQ Source")
//           .map(message -> parseMessage(message))  // 解析字节数组并提取信息
//           .print();
//
//        // 执行任务
//        env.execute("Flink ZeroMQ Consumer");
//    }
//
//    // 解析字节数组并提取关键信息
//    private static String parseMessage(byte[] message) {
//        int count = byteArrayToIntLittleEndian(message, 16);  // 获取车辆数量
//        StringBuilder result = new StringBuilder("Received message with count: ").append(count).append("\n");
//        for (int i = 0; i < count; i++) {
//            int offset = 20 + i * 44;  // 每个车辆数据占44字节
//        long carId = byteArrayToLongLittleEndian(message, offset);
//        String carNumber = new String(message, offset + 8, 16, StandardCharsets.UTF_8).trim();
//        byte type = message[offset + 24];
//        int[] scope = { byteArrayToIntLittleEndian(message, offset + 25), byteArrayToIntLittleEndian(message, offset + 29) };
//        float speed = Float.intBitsToFloat(byteArrayToIntLittleEndian(message, offset + 33));
//        byte wayno = message[offset + 37];
//        int tpointno = byteArrayToIntLittleEndian(message, offset + 38);
//        byte booleanValue = message[offset + 42];
//        byte direct = message[offset + 43];
//            result.append("carNumber:").append(carNumber).append("Car ID: ").append(carId)
//                  .append("type:").append(type).append("scope:").append(Arrays.toString(scope)).append(", wayno: ").append(wayno)
//                  .append(", Speed: ").append(speed).append(" m/s\n").append("tpointno: ").append(tpointno);
//        }
//        return result.toString();
//    }
//
//    // 辅助方法：从字节数组中解析整数（小端序）
//    private static int byteArrayToIntLittleEndian(byte[] bytes, int offset) {
//        return ((bytes[offset] & 0xFF) |
//                ((bytes[offset + 1] & 0xFF) << 8) |
//                ((bytes[offset + 2] & 0xFF) << 16) |
//                ((bytes[offset + 3] & 0xFF) << 24));
//    }
//
//    // 辅助方法：从字节数组中解析长整数（小端序）
//    private static long byteArrayToLongLittleEndian(byte[] bytes, int offset) {
//        return ((long) bytes[offset] & 0xFF) |
//                ((long) bytes[offset + 1] & 0xFF) << 8 |
//                ((long) bytes[offset + 2] & 0xFF) << 16 |
//                ((long) bytes[offset + 3] & 0xFF) << 24 |
//                ((long) bytes[offset + 4] & 0xFF) << 32 |
//                ((long) bytes[offset + 5] & 0xFF) << 40 |
//                ((long) bytes[offset + 6] & 0xFF) << 48 |
//                ((long) bytes[offset + 7] & 0xFF) << 56;
//    }
//}