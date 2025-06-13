package whu.edu.moniData.UDPRece;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class UDPReceiverKafka {
    private static final int PORT = 7171;
    private static final int BUFFER_SIZE = 4096;

    // 协议常量
    private static final byte[] START_FLAG = new byte[]{(byte) 0xFF, (byte) 0xFF};
    private static final byte END_FLAG = (byte) 0xFF;
    private static final byte MAIN_CMD = (byte) 0xE1;

    // 消息内容字段长度
    private static final int DEVICE_ID_LEN = 2;
    private static final int RESERVED1_LEN = 2;
    private static final int FRAME_NUM_LEN = 4;
    private static final int TIMESTAMP_SEC_LEN = 4;
    private static final int TIMESTAMP_MICROSEC_LEN = 4;
    private static final int LONGITUDE_LEN = 4;
    private static final int LATITUDE_LEN = 4;
    private static final int ANGLE_LEN = 2;
    private static final int PARTICIPANT_COUNT_LEN = 1;
    private static final int RESERVED2_LEN = 5;
    private static final int PARTICIPANT_SIZE = 116;
    private static final int CAMERA_TIMESTAMP_LEN = 8;
    private static final int RESERVED3_LEN = 4;
    // Kafka配置
    private static final String KAFKA_BROKERS = "100.65.38.40:9092";
    private static final String OUTPUT_TOPIC = "UDPDecoder";
    // 参与者感知信息类
    private static class ParticipantPerception {
        private byte[] id;
        private byte type;
        private byte confidence;
        private byte color;
        private byte source;
        private byte signBit;
        private byte cameraId;
        private byte[] longitude;
        private byte[] latitude;
        private byte[] altitude;
        private byte[] speed;
        private byte[] heading;
        private byte[] length;
        private byte[] width;
        private byte[] height;
        private byte[] x;
        private byte[] y;
        private byte[] z;
        private byte[] trackCount;

        public ParticipantPerception(byte[] content, int start) {
            int pos = start;
            this.id = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.type = content[pos++];
            this.confidence = content[pos++];
            this.color = content[pos++];
            this.source = content[pos++];
            this.signBit = content[pos++];
            this.cameraId = content[pos++];
            this.longitude = Arrays.copyOfRange(content, pos, pos + 4); pos += 4;
            this.latitude = Arrays.copyOfRange(content, pos, pos + 4); pos += 4;
            this.altitude = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.speed = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.heading = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.length = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.width = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.height = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.x = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.y = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.z = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.trackCount = Arrays.copyOfRange(content, pos, pos + 2);
        }

        public String toJsonString() {
            return "{" +
                    "\"id\":\"" + bytesToHex(id) + "\"," +
                    "\"type\":\"" + String.format("0x%02X", type) + "\"," +
                    "\"confidence\":" + (confidence & 0xFF) + "," +
                    "\"color\":\"" + String.format("0x%02X", color) + "\"," +
                    "\"source\":\"" + String.format("0x%02X", source) + "\"," +
                    "\"signBit\":\"" + String.format("0x%02X", signBit) + "\"," +
                    "\"cameraId\":" + (cameraId & 0xFF) + "," +
                    "\"longitude\":\"" + bytesToHex(longitude) + "\"," +
                    "\"latitude\":\"" + bytesToHex(latitude) + "\"," +
                    "\"altitude\":\"" + bytesToHex(altitude) + "\"," +
                    "\"speed\":\"" + bytesToHex(speed) + "\"," +
                    "\"heading\":\"" + bytesToHex(heading) + "\"," +
                    "\"length\":\"" + bytesToHex(length) + "\"," +
                    "\"width\":\"" + bytesToHex(width) + "\"," +
                    "\"height\":\"" + bytesToHex(height) + "\"," +
                    "\"x\":\"" + bytesToHex(x) + "\"," +
                    "\"y\":\"" + bytesToHex(y) + "\"," +
                    "\"z\":\"" + bytesToHex(z) + "\"," +
                    "\"trackCount\":\"" + bytesToHex(trackCount) + "\"" +
                    "}";
        }
    }

    // 参与者检测框类
    private static class DetectionBox {
        private byte[] topLeftX;
        private byte[] topLeftY;
        private byte[] bottomRightX;
        private byte[] bottomRightY;

        public DetectionBox(byte[] content, int start) {
            int pos = start;
            this.topLeftX = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.topLeftY = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.bottomRightX = Arrays.copyOfRange(content, pos, pos + 2); pos += 2;
            this.bottomRightY = Arrays.copyOfRange(content, pos, pos + 2);
        }

        public String toJsonString() {
            return "{" +
                    "\"topLeftX\":\"" + bytesToHex(topLeftX) + "\"," +
                    "\"topLeftY\":\"" + bytesToHex(topLeftY) + "\"," +
                    "\"bottomRightX\":\"" + bytesToHex(bottomRightX) + "\"," +
                    "\"bottomRightY\":\"" + bytesToHex(bottomRightY) + "\"" +
                    "}";
        }
    }

    // 完整参与者类
    private static class Participant {
        private ParticipantPerception fusion;
        private ParticipantPerception pointCloud;
        private ParticipantPerception video;
        private DetectionBox detectionBox;

        public Participant(byte[] content, int start) {
            this.fusion = new ParticipantPerception(content, start);
            this.pointCloud = new ParticipantPerception(content, start + 36);
            this.video = new ParticipantPerception(content, start + 72);
            this.detectionBox = new DetectionBox(content, start + 108);
        }

        public String toJsonString() {
            return "{" +
                    "\"fusion\":" + fusion.toJsonString() + "," +
                    "\"pointCloud\":" + pointCloud.toJsonString() + "," +
                    "\"video\":" + video.toJsonString() + "," +
                    "\"detectionBox\":" + detectionBox.toJsonString() +
                    "}";
        }
    }

    // 解析后的完整数据结构
    private static class ParsedData {
        private byte sequence;
        private byte mainCmd;
        private byte subCmd;
        private byte status;
        private int msgLength;
        private byte checksum;
        private String error;
        private byte[] deviceId;
        private byte[] reserved1;
        private byte[] frameNum;
        private byte[] timestampSec;
        private byte[] timestampMicrosec;
        private byte[] longitude;
        private byte[] latitude;
        private byte[] angle;
        private int participantCount;
        private byte[] reserved2;
        private final List<Participant> participants = new ArrayList<>();
        private byte[] frontCameraTs;
        private byte[] bodyCameraTs;
        private byte[] rearCameraTs;
        private byte[] reserved3;

        public void addParticipant(Participant participant) {
            participants.add(participant);
        }

        // 自定义JSON序列化方法
        public String toJson() {
            StringBuilder json = new StringBuilder();
            json.append("{");
            json.append("\"sequence\":").append(sequence & 0xFF).append(",");
            json.append("\"mainCmd\":\"").append(String.format("0x%02X", mainCmd)).append("\",");
            json.append("\"subCmd\":\"").append(String.format("0x%02X", subCmd)).append("\",");
            json.append("\"status\":").append(status & 0xFF).append(",");
            json.append("\"msgLength\":").append(msgLength).append(",");
            json.append("\"checksum\":").append(checksum & 0xFF).append(",");

            if (error != null) {
                json.append("\"error\":\"").append(error.replace("\"", "\\\"")).append("\",");
            }

            json.append("\"deviceId\":\"").append(bytesToHex(deviceId)).append("\",");
            json.append("\"reserved1\":\"").append(bytesToHex(reserved1)).append("\",");
            json.append("\"frameNum\":\"").append(bytesToHex(frameNum)).append("\",");
            json.append("\"timestampSec\":\"").append(bytesToHex(timestampSec)).append("\",");
            json.append("\"timestampMicrosec\":\"").append(bytesToHex(timestampMicrosec)).append("\",");
            json.append("\"longitude\":\"").append(bytesToHex(longitude)).append("\",");
            json.append("\"latitude\":\"").append(bytesToHex(latitude)).append("\",");
            json.append("\"angle\":\"").append(bytesToHex(angle)).append("\",");
            json.append("\"participantCount\":").append(participantCount).append(",");
            json.append("\"reserved2\":\"").append(bytesToHex(reserved2)).append("\",");

            // 序列化参与者列表
            json.append("\"participants\":[");
            for (int i = 0; i < participants.size(); i++) {
                json.append(participants.get(i).toJsonString());
                if (i < participants.size() - 1) {
                    json.append(",");
                }
            }
            json.append("],");

            json.append("\"frontCameraTs\":\"").append(bytesToHex(frontCameraTs)).append("\",");
            json.append("\"bodyCameraTs\":\"").append(bytesToHex(bodyCameraTs)).append("\",");
            json.append("\"rearCameraTs\":\"").append(bytesToHex(rearCameraTs)).append("\",");
            json.append("\"reserved3\":\"").append(bytesToHex(reserved3)).append("\"");

            json.append("}");
            return json.toString();
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

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

                // 解析协议
                ParsedData parsedData = parseProtocol(data);
                String jsonData = parsedData.toJson();
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
        } catch (SocketException e) {
            System.err.println("Socket error: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
        }
    }

    private static ParsedData parseProtocol(byte[] data) {
        ParsedData parsed = new ParsedData();

        // 1. 基本验证
        if (data.length < 12) {
            parsed.error = "Packet too short (" + data.length + " bytes)";
            return parsed;
        }

        // 2. 检查帧头和帧尾
        byte[] actualStart = Arrays.copyOfRange(data, 0, 2);
        byte actualEnd = data[data.length - 1];

        if (!Arrays.equals(actualStart, START_FLAG)) {
            parsed.error = "Invalid start flag: " + bytesToHex(actualStart);
            return parsed;
        }

        if (actualEnd != END_FLAG) {
            parsed.error = "Invalid end flag: " + String.format("0x%02X", actualEnd);
            return parsed;
        }

        // 3. 提取固定头部字段
        parsed.sequence = data[2];
        parsed.mainCmd = data[3];
        parsed.subCmd = data[4];
        parsed.status = data[5];
        byte[] msgLengthBytes = Arrays.copyOfRange(data, 6, 8);
        parsed.msgLength = ((msgLengthBytes[0] & 0xFF) << 8) | (msgLengthBytes[1] & 0xFF);
        int msgStart = 8;
        int msgEnd = msgStart + parsed.msgLength;
        int checksumPos = msgEnd;
        int endFlagPos = data.length - 1;

        // 6. 验证数据包完整性
        if (data.length < msgEnd + 2) {
            parsed.error = "Packet incomplete. Expected length: " + (msgEnd + 2) +
                    ", actual: " + data.length;
            return parsed;
        }

        parsed.checksum = data[checksumPos];

        // 7. 提取关键字段
        byte[] messageContent = Arrays.copyOfRange(data, msgStart, msgEnd);
        parseMessageContent(messageContent, parsed);

        return parsed;
    }

    private static void parseMessageContent(byte[] content, ParsedData parsed) {
        int pos = 0;

        // 1. 设备ID
        parsed.deviceId = Arrays.copyOfRange(content, pos, pos + DEVICE_ID_LEN);
        pos += DEVICE_ID_LEN;

        // 2. 预留位1
        parsed.reserved1 = Arrays.copyOfRange(content, pos, pos + RESERVED1_LEN);
        pos += RESERVED1_LEN;

        // 3. 点云帧号
        parsed.frameNum = Arrays.copyOfRange(content, pos, pos + FRAME_NUM_LEN);
        pos += FRAME_NUM_LEN;

        // 4. 时间戳秒
        parsed.timestampSec = Arrays.copyOfRange(content, pos, pos + TIMESTAMP_SEC_LEN);
        pos += TIMESTAMP_SEC_LEN;

        // 5. 时间戳微秒
        parsed.timestampMicrosec = Arrays.copyOfRange(content, pos, pos + TIMESTAMP_MICROSEC_LEN);
        pos += TIMESTAMP_MICROSEC_LEN;

        // 6. 经度
        parsed.longitude = Arrays.copyOfRange(content, pos, pos + LONGITUDE_LEN);
        pos += LONGITUDE_LEN;

        // 7. 纬度
        parsed.latitude = Arrays.copyOfRange(content, pos, pos + LATITUDE_LEN);
        pos += LATITUDE_LEN;

        // 8. 角度
        parsed.angle = Arrays.copyOfRange(content, pos, pos + ANGLE_LEN);
        pos += ANGLE_LEN;

        // 9. 参与者数量
        byte[] participantCount = Arrays.copyOfRange(content, pos, pos + PARTICIPANT_COUNT_LEN);
        pos += PARTICIPANT_COUNT_LEN;
        parsed.participantCount = participantCount[0] & 0xFF;

        // 10. 预留位2
        parsed.reserved2 = Arrays.copyOfRange(content, pos, pos + RESERVED2_LEN);
        pos += RESERVED2_LEN;

        // 11. 解析交通参与者
        for (int i = 0; i < parsed.participantCount; i++) {
            if (pos + PARTICIPANT_SIZE > content.length) {
                parsed.error = "Participant " + (i+1) + " incomplete";
                break;
            }

            Participant participant = new Participant(content, pos);
            parsed.addParticipant(participant);
            pos += PARTICIPANT_SIZE;
        }

        // 12. 相机时间戳
        if (pos + 3 * CAMERA_TIMESTAMP_LEN <= content.length) {
            parsed.frontCameraTs = Arrays.copyOfRange(content, pos, pos + CAMERA_TIMESTAMP_LEN);
            pos += CAMERA_TIMESTAMP_LEN;
            parsed.bodyCameraTs = Arrays.copyOfRange(content, pos, pos + CAMERA_TIMESTAMP_LEN);
            pos += CAMERA_TIMESTAMP_LEN;
            parsed.rearCameraTs = Arrays.copyOfRange(content, pos, pos + CAMERA_TIMESTAMP_LEN);
            pos += CAMERA_TIMESTAMP_LEN;
        }

        // 13. 预留位3
        if (pos + RESERVED3_LEN <= content.length) {
            parsed.reserved3 = Arrays.copyOfRange(content, pos, pos + RESERVED3_LEN);
        }
    }

    // 辅助方法：字节数组转十六进制字符串
    private static String bytesToHex(byte[] bytes) {
        if (bytes == null) return "";
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            hex.append(String.format("%02X", b));
        }
        return hex.toString();
    }
}