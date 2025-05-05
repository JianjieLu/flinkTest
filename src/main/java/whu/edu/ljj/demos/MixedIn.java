//package whu.edu.ljj.demos;
//
//import org.zeromq.ZContext;
//import org.zeromq.ZMQ;
//import org.zeromq.ZMsg;
//
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.nio.charset.StandardCharsets;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//
//
//public class MixedIn {
//    public static void main(String[] args) {
//        int n=1;
//          检查命令行参数
//        String publisherAddress = "tcp:100.65.62.82:8030";   发布者地址
//        int timeout = 5*1000;   超时时间（秒）
//        String outputFile = "D:\\轨迹存储论文\\123.txt";   输出文件名
//
//         创建ZMQ上下文和套接字
//        try (ZContext context = new ZContext();
//             FileWriter fileWriter = new FileWriter(outputFile);
//             PrintWriter printWriter = new PrintWriter(fileWriter)) {
//
//             创建订阅者套接字
//            ZMQ.Socket subscriber = context.createSocket(ZMQ.SUB);
//            subscriber.connect(publisherAddress);   连接到发布者
//            subscriber.subscribe("".getBytes());   订阅所有消息
//
//             设置接收超时时间
//
//import org.zeromq.ZContext;
//import org.zeromq.ZMQ;
//import org.zeromq.ZMsg;
//
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.PrintWriter;
//
//            subscriber.setReceiveTimeOut(timeout);
//
//            long startTime = System.currentTimeMillis();  //记录开始时间
//            while (!Thread.currentThread().isInterrupted()) {
//                 //接收消息
//                ZMsg msg = ZMsg.recvMsg(subscriber);
//
//                long elapsedTime = System.currentTimeMillis() - startTime;
//                String line = msg.toString();
//                line = line.replaceAll("[\\[\\]]", "").trim();
//                //my logic
//                byte[] data = hexStringToByteArray(line);
//                int sn = byteArrayToIntLittleEndian(data, 0);
//                String deviceIp = byteArrayToIp(data, 4);
//                long time = byteArrayToLongLittleEndian(data, 8);
//                int count = byteArrayToIntLittleEndian(data, 16);
//                 for (int i = 0; i < count; i++) {
//                    int offset = 20 + i * 44; // 每个TDATA占44字节
//                     long carId = byteArrayToLongLittleEndian(data, offset);
//                    String carNumber = new String(data, offset + 8, 16, StandardCharsets.UTF_8).trim();
//                    byte type = data[offset + 24];
//                    int[] scope = { byteArrayToIntLittleEndian(data, offset + 25), byteArrayToIntLittleEndian(data, offset + 29) };
//                    float speed = Float.intBitsToFloat(byteArrayToIntLittleEndian(data, offset + 33));
//                    byte wayno = data[offset + 37];
//                    int tpointno = byteArrayToIntLittleEndian(data, offset + 38);
//                    byte booleanValue = data[offset + 42];
//                    byte direct = data[offset + 43];
//
//                }
//                    // 时间格式化
//                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//                    String formattedTime = sdf.format(new Date(time));
//
//                    // 解析车辆数据
//                    List<String> carIds = new ArrayList<>();
//                    List<Float> speeds = new ArrayList<>();
//                    List<int[]> positions = new ArrayList<>();
//                    List<Integer> tpoints = new ArrayList<>();
//                    List<Byte> directs = new ArrayList<>();
//
//                    for (int i = 0; i < count; i++) {
//                        int offset = 20 + i * 44;
//
//                         //解析字段
//                        long carId = byteArrayToLongLittleEndian(data, offset);
//                        float speed = Float.intBitsToFloat(byteArrayToIntLittleEndian(data, offset + 33));
//                        int[] scope = {
//                            byteArrayToIntLittleEndian(data, offset + 25),
//                            byteArrayToIntLittleEndian(data, offset + 29)
//                        };
//                        int tpointno = byteArrayToIntLittleEndian(data, offset + 38);
//                        byte direct = data[offset + 43];
//
//                         //收集数据
//                        carIds.add(String.valueOf(carId));
//                        speeds.add(speed);
//                        positions.add(scope);
//                        tpoints.add(tpointno);
//                        directs.add(direct);
//                    }
//
//                     //按指定格式输出
//                    System.out.println("sn：" + sn);
//                    System.out.println("时间：" + formattedTime);
//                    System.out.println("设备ip：" + deviceIp);
//                    System.out.println("车辆数：" + count);
//                    System.out.println("两辆车的id：" );
//                    for(int i=0;i<count;i++){
//                        System.out.println(String.join("，", carIds));
//                    }
//                    System.out.println("两辆车的速度：" );
//                    for(int i=0;i<count;i++){
//                        System.out.println(String.format("%.2f", speeds.get(i)));
//                    }
//
//                    System.out.println("位置：[" + positions.get(0)[0] + "," + positions.get(0)[1] + "]，[" +
//                        positions.get(1)[0] + "," + positions.get(1)[1] + "]");
//                    System.out.println("里程：" + tpoints.get(0) + "，" + tpoints.get(1));
//                    System.out.println("方向：" + directs.get(0) + "，" + directs.get(1));
//                msg.destroy();  //释放消息资源
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//     public static void decodeRecord(String hex,int num) {
//        byte[] data = hexStringToByteArray(hex);
//
//         //解析SN, DEVICEIP, TIME, COUNT
//        int sn = byteArrayToIntLittleEndian(data, 0);
//        String deviceIp = byteArrayToIp(data, 4);
//        long time = byteArrayToLongLittleEndian(data, 8);
//        int count = byteArrayToIntLittleEndian(data, 16);
//         System.out.println();
//
//        System.out.println("SN: " + sn);
//        System.out.println("DEVICE IP: " + deviceIp);
//        System.out.println("TIME: " + time);
//        System.out.println("COUNT: " + count);
//
//         解析车辆信息
//        for (int i = 0; i < count; i++) {
//            int offset = 20 + i * 44;  每个TDATA占44字节
//            decodeVehicleData(data, offset);
//        }
//    }
//private
//    static void decodeVehicleData(byte[] data, int offset) {
//        long carId = byteArrayToLongLittleEndian(data, offset);
//        String carNumber = new String(data, offset + 8, 16, StandardCharsets.UTF_8).trim();
//        byte type = data[offset + 24];
//        int[] scope = { byteArrayToIntLittleEndian(data, offset + 25), byteArrayToIntLittleEndian(data, offset + 29) };
//        float speed = Float.intBitsToFloat(byteArrayToIntLittleEndian(data, offset + 33));
//        byte wayno = data[offset + 37];
//        int tpointno = byteArrayToIntLittleEndian(data, offset + 38);
//        byte booleanValue = data[offset + 42];
//        byte direct = data[offset + 43];
//
//        System.out.println("Car ID: " + carId);
//        System.out.println("Car Number: " + carNumber);
//        System.out.println("Type: " + type);
//        System.out.println("Scope: [" + scope[0] + ", " + scope[1] + "]");
//        System.out.println("Speed: " + speed + " m/s");
//        System.out.println("Wayno: " + wayno);
//        System.out.println("Tpointno: " + tpointno);
//        System.out.println("Boolean: " + booleanValue);
//        System.out.println("Direct: " + direct);
//         System.out.println("===============");
//
//    }
//
//    private static byte[] hexStringToByteArray(String s) {
//        int len = s.length();
//        byte[] data = new byte[len / 2];
//        for (int i = 0; i < len; i += 2) {
//            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
//                    + Character.digit(s.charAt(i+1), 16));
//        }
//        return data;
//    }
//
//    private static String byteArrayToIp(byte[] bytes, int offset) {
//        return ((bytes[offset] & 0xFF) + "." +
//                (bytes[offset + 1] & 0xFF) + "." +
//                (bytes[offset + 2] & 0xFF) + "." +
//                (bytes[offset + 3] & 0xFF));
//    }
//
//    private static int byteArrayToIntLittleEndian(byte[] bytes, int offset) {
//        return ((bytes[offset] & 0xFF) |
//                ((bytes[offset + 1] & 0xFF) << 8) |
//                ((bytes[offset + 2] & 0xFF) << 16) |
//                ((bytes[offset + 3] & 0xFF) << 24));
//    }
//
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
