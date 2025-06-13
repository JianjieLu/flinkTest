package whu.edu.moniData.UDPRece;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class UDPReceiverWithParser {

    private static final int PORT = 7171;
    private static final int BUFFER_SIZE = 4096;

    // Kafka配置
    private static final String KAFKA_BROKERS = "100.65.38.40:9092";
    private static final String OUTPUT_TOPIC = "smartBS_xg";

    // Gson实例用于JSON序列化
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             DatagramSocket socket = new DatagramSocket(PORT)) {

            System.out.println("UDP Receiver started on port " + PORT);
            System.out.println("Kafka Producer initialized. Brokers: " + KAFKA_BROKERS);
            byte[] buffer = new byte[BUFFER_SIZE];

            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                byte[] data = Arrays.copyOfRange(packet.getData(), 0, packet.getLength());
                System.out.println("Received " + data.length + " bytes from " +
                        packet.getAddress().getHostAddress() + ":" + packet.getPort());

                try {
                    // 解析协议
                    E1Frame frame = parse(data);

                    // 转换为UDPData格式
                    UDPData udpData = convertToUDPData(frame);
                    if(udpData!=null){
                        // 转换为JSON
                        String jsonData = gson.toJson(udpData);
                        System.out.println("Parsed JSON: " + jsonData);

                        // 发送到Kafka
                        ProducerRecord<String, String> record = new ProducerRecord<>(OUTPUT_TOPIC, jsonData);
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                System.err.println("Kafka send failed: " + exception.getMessage());
                            } else {
                                System.out.printf("Sent to Kafka -> Topic: %s, Partition: %d, Offset: %d%n",
                                        metadata.topic(), metadata.partition(), metadata.offset());
                            }
                        });
                    }

                } catch (Exception e) {
                    System.err.println("Error processing packet: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (SocketException e) {
            System.err.println("Socket error: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
        }
    }

    public static E1Frame parse(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        E1Frame frame = new E1Frame();

        // 解析帧头
        frame.startMarker = buffer.getShort() & 0xFFFF;
        frame.sequenceNumber = buffer.get() & 0xFF;
        frame.mainCommand = buffer.get() & 0xFF;
        frame.subCommand = buffer.get() & 0xFF;
        frame.voltState = buffer.get() & 0xFF;
        frame.messageLength = buffer.getShort() & 0xFFFF;

        // 检查子命令号，如果为2则跳过消息内容解析
        if (frame.subCommand == 5) {
            // 直接跳过整个消息内容部分
            buffer.position(buffer.position() + frame.messageLength);

            // 解析帧尾
            frame.checksum = buffer.get() & 0xFF;
            frame.endMarker = buffer.get() & 0xFF;

            return frame;
        }

        // 解析消息内容（仅当子命令不为2时执行）
        frame.deviceId = buffer.getShort() & 0xFFFF;
        frame.reserved1 = buffer.getShort() & 0xFFFF;
        frame.frameNumber = buffer.getInt();
        frame.timestampSec = buffer.getInt();
        frame.timestampUsec = buffer.getInt();
        frame.laserLongitude = buffer.getInt();
        frame.laserLatitude = buffer.getInt();
        frame.laserAngle = buffer.getShort() & 0xFFFF;
        frame.objectCount = buffer.get() & 0xFF;

        byte[] reserved2 = new byte[5];
        buffer.get(reserved2);
        frame.reserved2 = reserved2;

        // 解析交通参与者
        for (int i = 0; i < frame.objectCount; i++) {
            TrafficParticipant participant = new TrafficParticipant();

            // 融合部分
            participant.fused.id = buffer.getShort() & 0xFFFF;
            participant.fused.type = buffer.get() & 0xFF;
            participant.fused.confidence = buffer.get() & 0xFF;
            participant.fused.color = buffer.get() & 0xFF;
            participant.fused.source = buffer.get() & 0xFF;
            participant.fused.signBits = buffer.get() & 0xFF;
            participant.fused.cameraId = buffer.get() & 0xFF;
            participant.fused.longitude = buffer.getInt();
            participant.fused.latitude = buffer.getInt();
            participant.fused.altitude = buffer.getShort();
            participant.fused.speed = buffer.getShort() & 0xFFFF;
            participant.fused.heading = buffer.getShort() & 0xFFFF;
            participant.fused.length = buffer.getShort() & 0xFFFF;
            participant.fused.width = buffer.getShort() & 0xFFFF;
            participant.fused.height = buffer.getShort() & 0xFFFF;
            participant.fused.x = buffer.getShort();
            participant.fused.y = buffer.getShort();
            participant.fused.z = buffer.getShort();
            participant.fused.hitCount = buffer.getShort() & 0xFFFF;

            // 点云部分
            participant.pointCloud.id = buffer.getShort() & 0xFFFF;
            participant.pointCloud.type = buffer.get() & 0xFF;
            participant.pointCloud.confidence = buffer.get() & 0xFF;
            participant.pointCloud.color = buffer.get() & 0xFF;
            participant.pointCloud.source = buffer.get() & 0xFF;
            participant.pointCloud.signBits = buffer.get() & 0xFF;
            participant.pointCloud.cameraId = buffer.get() & 0xFF;
            participant.pointCloud.longitude = buffer.getInt();
            participant.pointCloud.latitude = buffer.getInt();
            participant.pointCloud.altitude = buffer.getShort();
            participant.pointCloud.speed = buffer.getShort() & 0xFFFF;
            participant.pointCloud.heading = buffer.getShort() & 0xFFFF;
            participant.pointCloud.length = buffer.getShort() & 0xFFFF;
            participant.pointCloud.width = buffer.getShort() & 0xFFFF;
            participant.pointCloud.height = buffer.getShort() & 0xFFFF;
            participant.pointCloud.x = buffer.getShort();
            participant.pointCloud.y = buffer.getShort();
            participant.pointCloud.z = buffer.getShort();
            participant.pointCloud.hitCount = buffer.getShort() & 0xFFFF;

            // 视频部分
            participant.video.id = buffer.getShort() & 0xFFFF;
            participant.video.type = buffer.get() & 0xFF;
            participant.video.confidence = buffer.get() & 0xFF;
            participant.video.color = buffer.get() & 0xFF;
            participant.video.source = buffer.get() & 0xFF;
            participant.video.signBits = buffer.get() & 0xFF;
            participant.video.cameraId = buffer.get() & 0xFF;
            participant.video.longitude = buffer.getInt();
            participant.video.latitude = buffer.getInt();
            participant.video.altitude = buffer.getShort();
            participant.video.speed = buffer.getShort() & 0xFFFF;
            participant.video.heading = buffer.getShort() & 0xFFFF;
            participant.video.length = buffer.getShort() & 0xFFFF;
            participant.video.width = buffer.getShort() & 0xFFFF;
            participant.video.height = buffer.getShort() & 0xFFFF;
            participant.video.x = buffer.getShort();
            participant.video.y = buffer.getShort();
            participant.video.z = buffer.getShort();
            participant.video.hitCount = buffer.getShort() & 0xFFFF;

            // 检测框
            participant.bbox.topLeftX = buffer.getShort() & 0xFFFF;
            participant.bbox.topLeftY = buffer.getShort() & 0xFFFF;
            participant.bbox.bottomRightX = buffer.getShort() & 0xFFFF;
            participant.bbox.bottomRightY = buffer.getShort() & 0xFFFF;

            frame.participants.add(participant);
        }

        // 解析时间戳信息
        frame.frontCameraTimestamp = buffer.getLong();
        frame.bodyCameraTimestamp = buffer.getLong();
        frame.rearCameraTimestamp = buffer.getLong();

        byte[] reserved3 = new byte[4];
        buffer.get(reserved3);
        frame.reserved3 = reserved3;

        // 解析帧尾
        frame.checksum = buffer.get() & 0xFF;
        frame.endMarker = buffer.get() & 0xFF;

        return frame;
    }

    // 将解析的E1Frame转换为文档要求的UDPData格式
    public static UDPData convertToUDPData(E1Frame frame) {
        UDPData udpData = new UDPData();
        DecimalFormat df = new DecimalFormat("0.0000000");
        DecimalFormat df1 = new DecimalFormat("0.00");
        if(frame.objectCount==0)return null;
        // 基本帧信息
        udpData.sequence = frame.sequenceNumber;
        udpData.mainCmd = frame.mainCommand;
        udpData.subCmd = frame.subCommand;
        udpData.status = frame.voltState;
        udpData.msgLength = frame.messageLength;
        udpData.deviceId = frame.deviceId;
        udpData.reserved1 = frame.reserved1;
        udpData.frameNum = frame.frameNumber;
        udpData.timestampMicrosec = frame.timestampSec * 1000L + frame.timestampUsec / 1000;;
        // 位置信息（转换为文档要求的double/float类型）
        udpData.longitude = Double.parseDouble(df.format(frame.laserLongitude * 1e-7)); // 转换为度
        udpData.latitude = Double.parseDouble(df.format(frame.laserLatitude * 1e-7));   // 转换为度
        udpData.angle = frame.laserAngle; // 直接使用原始值

        // 参与者信息
        udpData.participantCount = frame.objectCount;

        // 处理预留字段（取前4字节）
        if (frame.reserved2 != null && frame.reserved2.length >= 4) {
            udpData.reserved2 = ByteBuffer.wrap(frame.reserved2, 0, 4)
                    .order(ByteOrder.BIG_ENDIAN).getInt();
        }

        // 转换参与者列表
        for (TrafficParticipant tp : frame.participants) {
            Participant p = new Participant();
            ObjectData fused = tp.fused; // 使用融合数据部分

            p.id = fused.id;
            p.type = fused.type;
            p.confidence = fused.confidence; // 置信度（百分比值）
            p.color = fused.color;
            p.source = fused.source;
            p.signBit = fused.signBits;
            p.cameraId = fused.cameraId;

            // 位置信息转换
            p.longitude = Double.parseDouble(df.format(fused.longitude * 1e-7)); // 转换为度
            p.latitude = Double.parseDouble(df.format(fused.latitude * 1e-7));   // 转换为度
            p.altitude = Float.parseFloat(df1.format(fused.altitude));          // 海拔（米）

            // 运动信息（假设原始数据单位是0.01，转换为标准单位）
            p.speed = Float.parseFloat(df1.format( fused.speed * 0.01f));        // 转换为m/s
            p.heading =  Float.parseFloat(df1.format(fused.heading * 0.01f));     // 转换为度

            // 尺寸信息
            p.length = Float.parseFloat(df1.format( fused.length * 0.01f));       // 转换为米
            p.width =  Float.parseFloat(df1.format(fused.width * 0.01f));         // 转换为米
            p.height = Float.parseFloat(df1.format( fused.height * 0.01f));       // 转换为米

            // 坐标信息
            p.X =  Float.parseFloat(df1.format(fused.x * 0.01f));                // 转换为米
            p.Y = Float.parseFloat(df1.format( fused.y * 0.01f));                // 转换为米
            p.Z =  Float.parseFloat(df1.format(fused.z * 0.01f));                // 转换为米

            p.trackCount = fused.hitCount;         // 跟踪击中次数

            udpData.participants.add(p);
        }

        // 相机时间戳
        udpData.frontCameraTs = frame.frontCameraTimestamp;
        udpData.bodyCameraTs = frame.bodyCameraTimestamp;
        udpData.rearCameraTs = frame.rearCameraTimestamp;

        // 处理预留字段
        if (frame.reserved3 != null && frame.reserved3.length == 4) {
            udpData.reserved3 = ByteBuffer.wrap(frame.reserved3)
                    .order(ByteOrder.BIG_ENDIAN).getInt();
        }

        // 校验和
        udpData.checksum = frame.checksum;

        return udpData;
    }

    // 文档要求的UDPData格式
    static class UDPData {
        long sequence;
        int mainCmd;
        int subCmd;
        int status;
        int msgLength;
        int deviceId;
        int reserved1;
        long frameNum;
        long timestampMicrosec;
        double longitude;
        double latitude;
        float angle;
        int participantCount;
        int reserved2;
        List<Participant> participants = new ArrayList<>();
        long frontCameraTs;
        long bodyCameraTs;
        long rearCameraTs;
        int reserved3;
        int checksum;
    }

    // 文档要求的Participant格式
    static class Participant {
        long id;
        int type;
        float confidence;
        int color;
        int source;
        int signBit;
        int cameraId;
        double longitude;
        double latitude;
        float altitude;
        float speed;
        float heading;
        float length;
        float width;
        float height;
        float X;
        float Y;
        float Z;
        int trackCount;
    }

    // 原始解析数据结构
    static class E1Frame {
        // 帧头部分
        int startMarker;
        int sequenceNumber;
        int mainCommand;
        int subCommand;
        int voltState;
        int messageLength;

        // 消息内容
        int deviceId;
        int reserved1;
        long frameNumber;
        long timestampSec;
        long timestampUsec;
        int laserLongitude;
        int laserLatitude;
        int laserAngle;
        int objectCount;
        byte[] reserved2;

        // 交通参与者列表
        List<TrafficParticipant> participants = new ArrayList<>();

        // 相机时间戳
        long frontCameraTimestamp;
        long bodyCameraTimestamp;
        long rearCameraTimestamp;
        byte[] reserved3;

        // 帧尾
        int checksum;
        int endMarker;
    }

    static class TrafficParticipant {
        ObjectData fused = new ObjectData();
        ObjectData pointCloud = new ObjectData();
        ObjectData video = new ObjectData();
        BoundingBox bbox = new BoundingBox();
    }

    static class ObjectData {
        int id;
        int type;
        int confidence;
        int color;
        int source;
        int signBits;
        int cameraId;
        int longitude;
        int latitude;
        short altitude;
        int speed;
        int heading;
        int length;
        int width;
        int height;
        short x;
        short y;
        short z;
        int hitCount;
    }

    static class BoundingBox {
        int topLeftX;
        int topLeftY;
        int bottomRightX;
        int bottomRightY;
    }
}