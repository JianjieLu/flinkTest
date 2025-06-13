//package whu.edu.moniData.Utils;
//import lombok.*;
//import java.io.IOException;
//import java.net.DatagramPacket;
//import java.net.DatagramSocket;
//import java.net.InetAddress;
//import java.nio.ByteBuffer;
//import java.nio.ByteOrder;
//import java.time.Instant;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.time.format.DateTimeFormatter;
//import java.util.ArrayList;
//import java.util.List;
//import whu.edu.ljj.flink.xiaohanying.Utils.*;
//public class UdpStationReceiver {
//
//    public static void main(String[] args) throws IOException {
//        String address = "100.65.38.38";
//        int port = 7171;
//        DatagramSocket socket = new DatagramSocket(port, InetAddress.getByName(address));
//        byte[] buffer = new byte[4096];
//
//        while (true) {
//            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
//            socket.receive(packet);
//            byte[] data = packet.getData();
//            int length = packet.getLength();
//
//            // 校验帧头和帧尾
//            if ((data[0] & 0xFF) != 0xFF || (data[1] & 0xFF) != 0xFF || (data[length - 1] & 0xFF) != 0xFF) {
//                System.err.println("Invalid frame start/end markers");
//                continue;
//            }
//
//            // BCC校验（简单示例，需根据协议实现）
//            byte calculatedBcc = 0;
//            for (int i = 2; i < length - 2; i++) {
//                calculatedBcc ^= data[i];
//            }
//            if (calculatedBcc != data[length - 2]) {
//                System.err.println("BCC check failed");
//                continue;
//            }
//
//            // 解析主命令号和子命令号
//            int mainCmd = data[3] & 0xFF;
//            int subCmd = data[4] & 0xFF;
//            if (mainCmd != 0xE1 || subCmd != 0x03) {
//                System.err.println("Unsupported command");
//                continue;
//            }
//
//            // 解析消息长度（大端）
//            int msgLength = ((data[6] & 0xFF) << 8) | (data[7] & 0xFF);
//
//            // 解析消息内容（从索引8开始）
//            ByteBuffer msgBuffer = ByteBuffer.wrap(data, 8, msgLength).order(ByteOrder.BIG_ENDIAN);
//            stationDataOri sta = parseMessageContent(msgBuffer);
//
//            // TODO: 处理stationDataOri（如发送到Kafka或存储）
//            System.out.println("Parsed data: " + sta);
//        }
//    }
//
//    private static stationDataOri parseMessageContent(ByteBuffer buffer) {
//        stationDataOri data = new stationDataOri();
//
//        // 设备ID、预留位等跳过
//        buffer.position(buffer.position() + 4);
//
//        // 点云帧号（uint32）
//        data.setFrameNum(buffer.getInt());
//
//        // 时间戳秒（uint32）和微秒（uint32）
//        long unixSeconds = Integer.toUnsignedLong(buffer.getInt());
//        long micros = Integer.toUnsignedLong(buffer.getInt());
//        data.setGlobalTime(formatUnixTime(unixSeconds, micros));
//        data.setKafkaTime(System.currentTimeMillis());
//
//        // 激光器原点经纬度（uint32）
//        double originLon = buffer.getInt() * 1e-7;
//        double originLat = buffer.getInt() * 1e-7;
//
//        // 激光器角度等跳过
//        buffer.position(buffer.position() + 2 + 1 + 5);
//
//        // 交通参与者数量
//        int targetCount = buffer.get() & 0xFF;
//        List<StationTarget> targets = new ArrayList<>();
//
//        for (int i = 0; i < targetCount; i++) {
//            StationTarget target = new StationTarget();
//
//            // 融合目标解析（示例仅解析部分字段）
//            target.setId(buffer.getShort() & 0xFFFF);
//            target.setCarType(buffer.get() & 0xFF);
//            buffer.get(); // 置信度
//            target.setCarColor(buffer.get() & 0xFF);
//            buffer.position(buffer.position() + 4); // 跳过其他字段
//
//            // 经度纬度（nint32_t，有符号）
//            target.setLon(buffer.getInt() * 1e-7);
//            target.setLat(buffer.getInt() * 1e-7);
//
//            // 速度（cm/s转m/s）
//            target.setSpeed((buffer.getShort() & 0xFFFF) / 100.0f);
//
//            // 坐标（示例仅解析XYZ）
//            target.setAxisX(buffer.getShort() & 0xFFFF);
//            target.setAxisY(buffer.getShort() & 0xFFFF);
//            target.setAxisZ(buffer.getShort() & 0xFFFF);
//
//            // 其他字段跳过（根据协议调整）
//            buffer.position(buffer.position() + 44);
//
//            targets.add(target);
//        }
//
//        data.setTargetList(targets);
//        data.setOrgCode("DEFAULT_ORG"); // 根据协议填充实际值
//        return data;
//    }
//
//    private static String formatUnixTime(long seconds, long micros) {
//        Instant instant = Instant.ofEpochSecond(seconds).plusNanos(micros * 1000);
//        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
//                LocalDateTime.ofInstant(instant, ZoneId.systemDefault()));
//    }
//
//
//}