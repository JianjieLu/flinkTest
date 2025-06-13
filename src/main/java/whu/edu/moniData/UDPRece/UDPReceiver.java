package whu.edu.moniData.UDPRece;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Arrays;

public class UDPReceiver {
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

    // 参与者字段类型
    private static final String[] FUSION_TYPES = {"未知", "汽车", "卡车/货车", "大巴车", "行人", "自行车", "摩托车/电动车", "中巴车"};
    private static final String[] POINT_CLOUD_TYPES = {"car", "bicycle", "small_bus", "big_bus", "motorbike", "person",
            "large_truck", "small_truck", "middle_truck", "large_truck",
            "roadblock", "construction_sign_board", "spillage", "rider"};
    private static final String[] COLORS = {"未知", "白色", "黑色", "红色", "银色", "黄色", "蓝色", "彩色/杂色", "棕色", "灰色"};
    private static final String[] SOURCES = {"激光", "视频", "视频激光融合"};
    private static final String[] CAMERA_IDS = {"无", "车头", "车身", "车尾"};

    public static void main(String[] args) {
        try (DatagramSocket socket = new DatagramSocket(PORT)) {
            System.out.println("UDP Receiver started on port " + PORT);
            byte[] buffer = new byte[BUFFER_SIZE];

//            while (true) {
//                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
//                socket.receive(packet);

//                byte[] data = Arrays.copyOfRange(packet.getData(), 0, packet.getLength());
//                System.out.println("Received " + data.length + " bytes from " +
//                        packet.getAddress().getHostAddress() + ":" + packet.getPort());
//                byte[] data = {-1, -1, 0, -31, 2, 0, 1, 46, 0, 7, 0, 0, 0, 0, 0, 0, 104, 71, -35, -86, 0, 8, -23, 64, 18, 109, -31, 31, 18, 109, -31, 31, 0, 0, 2, 0, 0, 0, 0, 0, 122, 124, 0, 0, 0, 2, 2, 0, 67, -8, -49, 119, 18, 109, -28, -66, 0, 0, 5, -100, 1, 13, 1, -59, 0, -70, 0, -94, 29, -105, 4, -119, -2, 84, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 122, 125, 0, 0, 0, 2, 3, 0, 67, -8, -15, 8, 18, 109, -29, -42, 0, 0, 6, 41, 1, 14, 1, -72, 0, -74, 0, -94, 2, 113, 2, -8, -2, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -55, -1};
            byte[] data = {-1, -1, 0, -31, 2, 0, 1, 46,
                    0, 10, //id 2
                    0, 0, //预留位
                    0, 0, 0, 0, //帧号
                    104, 71, -35, -56, //时间戳秒
                    0, 3, -28, 24, //时间戳微妙
                    18, 109, -34, 52,//激光经度
                    18, 109, -34, 52, //激光纬度
                    0, 0, //角度
                    2, //数量
                    0, 0, 0, 0, 0, //预留

                    -92, -86,//,id
                    6,//类型
                    0,//置信度
                    0,//颜色
                    2,//来源
                    2,//符号位
                    0,//所在相机
                    67, -7, 62, -24,//经度
                    18, 109, -27, 23,//纬度
                    0, 0, //海拔
                    4, -125, //速度
                    1, 14, //航向角
                    4, 12, //长度
                    1, 10, //宽度
                    1, 100, //高度
                    22, -80, //x
                    8, -92, //y
                    -2, 95,//z
                    0, 0,//次数

                    0, 0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,



                    0, 0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,


                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,

                    -92, -84,
                    0,
                    0,
                    0,
                    2,
                    3,
                    0,
                    67, -7, 119, -93,
                    18, 109, -27, 68,
                    0, 0,
                    6, 104,
                    1, 10,
                    1, -107,
                    0, -88,
                    0, -110,
                    31, 114,
                    6, 121,
                    -2, 96,
                    0, 0,

                    0, 0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,

                    0, 0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,


                    0, 0,
                    0, 0,
                    0, 0,
                    0, 0,

                    0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0,

                    -97, -1};
                // 解析协议
                parseProtocol(data);
//            }
        } catch (SocketException e) {
            System.err.println("Socket error: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
        }
    }

    private static void parseProtocol(byte[] data) {
        // 1. 基本验证
        if (data.length < 12) {
            System.out.println("Error: Packet too short (" + data.length + " bytes)");
            return;
        }

        // 2. 检查帧头和帧尾
        byte[] actualStart = Arrays.copyOfRange(data, 0, 2);
        byte actualEnd = data[data.length - 1];

        if (!Arrays.equals(actualStart, START_FLAG)) {
            System.out.println("Invalid start flag: " + bytesToHex(actualStart));
            return;
        }

        if (actualEnd != END_FLAG) {
            System.out.println("Invalid end flag: " + String.format("0x%02X", actualEnd));
            return;
        }

        // 3. 提取固定头部字段
        byte sequence = data[2];
        byte mainCmd = data[3];
        byte subCmd = data[4];
        byte status = data[5];
        byte[] msgLengthBytes = Arrays.copyOfRange(data, 6, 8);

        // 4. 验证主命令号
        if (mainCmd != MAIN_CMD) {
            System.out.println("Invalid main command: " + String.format("0x%02X", mainCmd));
            return;
        }

        // 5. 计算消息内容位置
        int msgLength = ((msgLengthBytes[0] & 0xFF) << 8) | (msgLengthBytes[1] & 0xFF);
        int msgStart = 8;
        int msgEnd = msgStart + msgLength;
        int checksumPos = msgEnd;
        int endFlagPos = data.length - 1;

        // 6. 验证数据包完整性
        if (data.length < msgEnd + 2) {
            System.out.println("Packet incomplete. Expected length: " + (msgEnd + 2) +
                    ", actual: " + data.length);
            return;
        }

        // 7. 提取关键字段
        byte[] messageContent = Arrays.copyOfRange(data, msgStart, msgEnd);
        byte checksum = data[checksumPos];

        // 8. 打印头部信息
        System.out.println("\n===== Protocol Header =====");
        System.out.println("Start Flag: " + bytesToHex(actualStart));
        System.out.println("Sequence: " + sequence);
        System.out.println("Main Cmd: 0x" + String.format("%02X", mainCmd) + " (E1)");
        System.out.println("Sub Cmd: " + subCmd);
        System.out.println("Status: " + status);
        System.out.println("Msg Length: " + msgLength + " bytes");
        System.out.println("Checksum: " + checksum);
        System.out.println("End Flag: " + bytesToHex(new byte[]{actualEnd}));

        // 9. 解析消息内容
        parseMessageContent(messageContent);
    }

    private static void parseMessageContent(byte[] content) {
        System.out.println("\n===== Message Content =====");
        int pos = 0;

        // 1. 设备ID
        byte[] deviceId = Arrays.copyOfRange(content, pos, pos + DEVICE_ID_LEN);
        pos += DEVICE_ID_LEN;
        System.out.println("Device ID: " + bytesToHex(deviceId) + " (十进制: " + bytesToInt(deviceId) + ")");

        // 2. 预留位1
        byte[] reserved1 = Arrays.copyOfRange(content, pos, pos + RESERVED1_LEN);
        pos += RESERVED1_LEN;
        System.out.println("Reserved1: " + bytesToHex(reserved1));

        // 3. 点云帧号
        byte[] frameNum = Arrays.copyOfRange(content, pos, pos + FRAME_NUM_LEN);
        pos += FRAME_NUM_LEN;
        System.out.println("Point Cloud Frame#: " + bytesToHex(frameNum) + " (十进制: " + bytesToInt(frameNum) + ")");

        // 4. 时间戳秒
        byte[] timestampSec = Arrays.copyOfRange(content, pos, pos + TIMESTAMP_SEC_LEN);
        pos += TIMESTAMP_SEC_LEN;
        long unixTimeSec = bytesToUnsignedInt(timestampSec);
        System.out.println("Timestamp (sec): " + unixTimeSec + " (Unix时间)");

        // 5. 时间戳微秒
        byte[] timestampMicrosec = Arrays.copyOfRange(content, pos, pos + TIMESTAMP_MICROSEC_LEN);
        pos += TIMESTAMP_MICROSEC_LEN;
        long unixTimeMicrosec = bytesToUnsignedInt(timestampMicrosec);
        System.out.println("Timestamp (usec): " + unixTimeMicrosec + " μs");

        // 6. 经度
        byte[] longitude = Arrays.copyOfRange(content, pos, pos + LONGITUDE_LEN);
        pos += LONGITUDE_LEN;
        double longitudeValue = bytesToCoordinate(longitude);
        System.out.println("Longitude: " + longitudeValue + "° (东经为正)");

        // 7. 纬度
        byte[] latitude = Arrays.copyOfRange(content, pos, pos + LATITUDE_LEN);
        pos += LATITUDE_LEN;
        double latitudeValue = bytesToCoordinate(latitude);
        System.out.println("Latitude: " + latitudeValue + "° (北纬为正)");

        // 8. 角度
        byte[] angle = Arrays.copyOfRange(content, pos, pos + ANGLE_LEN);
        pos += ANGLE_LEN;
        int angleValue = bytesToUnsignedShort(angle);
        System.out.println("Angle: " + angleValue + "° (正北为0°)");

        // 9. 参与者数量
        byte[] participantCount = Arrays.copyOfRange(content, pos, pos + PARTICIPANT_COUNT_LEN);
        pos += PARTICIPANT_COUNT_LEN;
        int count = participantCount[0] & 0xFF;
        System.out.println("Participant Count: " + count);

        // 10. 预留位2
        byte[] reserved2 = Arrays.copyOfRange(content, pos, pos + RESERVED2_LEN);
        pos += RESERVED2_LEN;
        System.out.println("Reserved2: " + bytesToHex(reserved2));

        // 11. 解析交通参与者
        System.out.println("\n===== Traffic Participants =====");
        for (int i = 0; i < count; i++) {
            if (pos + PARTICIPANT_SIZE > content.length) {
                System.out.println("Warning: Participant " + (i+1) + " incomplete");
                break;
            }

            System.out.println("\nParticipant " + (i+1) + ":");

            // 解析融合部分 (36字节)
            System.out.println("  [Fusion]");
            parseFusionInfo(content, pos);
            pos += 36;

            // 解析点云部分 (36字节)
            System.out.println("  [Point Cloud]");
            parsePointCloudInfo(content, pos);
            pos += 36;

            // 解析视频部分 (36字节)
            System.out.println("  [Video]");
            parseVideoInfo(content, pos);
            pos += 36;

            // 解析视频检测框 (8字节)
            System.out.println("  [Detection Box]");
            parseDetectionBox(content, pos);
            pos += 8;
        }

        // 12. 相机时间戳
        System.out.println("\n===== Camera Timestamps =====");
        if (pos + 3 * CAMERA_TIMESTAMP_LEN <= content.length) {
            byte[] frontTs = Arrays.copyOfRange(content, pos, pos + CAMERA_TIMESTAMP_LEN);
            pos += CAMERA_TIMESTAMP_LEN;
            System.out.println("Front Camera: " + bytesToLong(frontTs) + " ms");

            byte[] bodyTs = Arrays.copyOfRange(content, pos, pos + CAMERA_TIMESTAMP_LEN);
            pos += CAMERA_TIMESTAMP_LEN;
            System.out.println("Body Camera: " + bytesToLong(bodyTs) + " ms");

            byte[] rearTs = Arrays.copyOfRange(content, pos, pos + CAMERA_TIMESTAMP_LEN);
            pos += CAMERA_TIMESTAMP_LEN;
            System.out.println("Rear Camera: " + bytesToLong(rearTs) + " ms");
        }

        // 13. 预留位3
        if (pos + RESERVED3_LEN <= content.length) {
            byte[] reserved3 = Arrays.copyOfRange(content, pos, pos + RESERVED3_LEN);
            System.out.println("\nReserved3: " + bytesToHex(reserved3));
        }

        System.out.println("\n===== End of Message =====");
    }

    private static void parseFusionInfo(byte[] content, int start) {
        int pos = start;

        // ID
        byte[] id = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        System.out.println("    ID: " + bytesToHex(id) + " (十进制: " + bytesToInt(id) + ")");

        // 类型
        byte type = content[pos++];
        String typeStr = type < FUSION_TYPES.length ? FUSION_TYPES[type] : "未知类型";
        System.out.println("    Type: " + type + " (" + typeStr + ")");

        // 置信度
        byte confidence = content[pos++];
        System.out.println("    Confidence: " + (confidence & 0xFF) + "%");

        // 颜色
        byte color = content[pos++];
        String colorStr = color < COLORS.length ? COLORS[color] : "未知颜色";
        System.out.println("    Color: " + color + " (" + colorStr + ")");

        // 信息来源
        byte source = content[pos++];
        String sourceStr = source < SOURCES.length ? SOURCES[source] : "未知来源";
        System.out.println("    Source: " + source + " (" + sourceStr + ")");

        // 符号位
        byte signBit = content[pos++];
        System.out.println("    Sign Bit: " + String.format("0x%02X", signBit));

        // 相机ID
        byte cameraId = content[pos++];
        String cameraStr = cameraId < CAMERA_IDS.length ? CAMERA_IDS[cameraId] : "未知相机";
        System.out.println("    Camera ID: " + cameraId + " (" + cameraStr + ")");

        // 经度
        byte[] longitude = Arrays.copyOfRange(content, pos, pos + 4);
        pos += 4;
        double longitudeValue = bytesToCoordinate(longitude);
        System.out.println("    Longitude: " + longitudeValue + "°");

        // 纬度
        byte[] latitude = Arrays.copyOfRange(content, pos, pos + 4);
        pos += 4;
        double latitudeValue = bytesToCoordinate(latitude);
        System.out.println("    Latitude: " + latitudeValue + "°");

        // 海拔
        byte[] altitude = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int altitudeValue = bytesToShort(altitude);
        System.out.println("    Altitude: " + altitudeValue + " cm");

        // 速度
        byte[] speed = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int speedValue = bytesToUnsignedShort(speed);
        System.out.println("    Speed: " + speedValue + " cm/s");

        // 航向角
        byte[] heading = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int headingValue = bytesToUnsignedShort(heading);
        System.out.println("    Heading: " + headingValue + "°");

        // 长度
        byte[] lengthBytes = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int lengthValue = bytesToUnsignedShort(lengthBytes);
        System.out.println("    Length: " + lengthValue + " cm");

        // 宽度
        byte[] width = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int widthValue = bytesToUnsignedShort(width);
        System.out.println("    Width: " + widthValue + " cm");

        // 高度
        byte[] height = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int heightValue = bytesToUnsignedShort(height);
        System.out.println("    Height: " + heightValue + " cm");

        // X坐标
        byte[] x = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int xValue = bytesToShort(x);
        System.out.println("    X: " + xValue + " cm");

        // Y坐标
        byte[] y = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int yValue = bytesToShort(y);
        System.out.println("    Y: " + yValue + " cm");

        // Z坐标
        byte[] z = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int zValue = bytesToShort(z);
        System.out.println("    Z: " + zValue + " cm");

        // 跟踪击中次数
        byte[] trackCount = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        System.out.println("    Track Count: " + bytesToHex(trackCount));
    }

    private static void parsePointCloudInfo(byte[] content, int start) {
        int pos = start;

        // ID
        byte[] id = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        System.out.println("    ID: " + bytesToHex(id) + " (十进制: " + bytesToInt(id) + ")");

        // 类型
        byte type = content[pos++];
        String typeStr = "未知类型";
        if (type == 0) typeStr = "car";
        else if (type == 1) typeStr = "bicycle";
        else if (type == 201) typeStr = "small_bus";
        else if (type == 202) typeStr = "big_bus";
        else if (type == 3) typeStr = "motorbike";
        else if (type == 4) typeStr = "person";
        else if (type == 5) typeStr = "large_truck";
        else if (type == 6) typeStr = "small_truck";
        else if (type == 7) typeStr = "middle_truck";
        else if (type == 8) typeStr = "large_truck";
        else if (type == 13) typeStr = "roadblock";
        else if (type == 15) typeStr = "construction_sign_board";
        else if (type == 16) typeStr = "spillage";
        else if (type == 17) typeStr = "rider";
        System.out.println("    Type: " + type + " (" + typeStr + ")");

        // 置信度
        byte confidence = content[pos++];
        System.out.println("    Confidence: " + (confidence & 0xFF) + "%");

        // 颜色
        byte color = content[pos++];
        String colorStr = color < COLORS.length ? COLORS[color] : "未知颜色";
        System.out.println("    Color: " + color + " (" + colorStr + ")");

        // 信息来源
        byte source = content[pos++];
        String sourceStr = source < SOURCES.length ? SOURCES[source] : "未知来源";
        System.out.println("    Source: " + source + " (" + sourceStr + ")");

        // 符号位
        byte signBit = content[pos++];
        System.out.println("    Sign Bit: " + String.format("0x%02X", signBit));

        // 相机ID
        byte cameraId = content[pos++];
        String cameraStr = cameraId < CAMERA_IDS.length ? CAMERA_IDS[cameraId] : "未知相机";
        System.out.println("    Camera ID: " + cameraId + " (" + cameraStr + ")");

        // 经度
        byte[] longitude = Arrays.copyOfRange(content, pos, pos + 4);
        pos += 4;
        double longitudeValue = bytesToCoordinate(longitude);
        System.out.println("    Longitude: " + longitudeValue + "°");

        // 纬度
        byte[] latitude = Arrays.copyOfRange(content, pos, pos + 4);
        pos += 4;
        double latitudeValue = bytesToCoordinate(latitude);
        System.out.println("    Latitude: " + latitudeValue + "°");

        // 海拔
        byte[] altitude = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int altitudeValue = bytesToShort(altitude);
        System.out.println("    Altitude: " + altitudeValue + " cm");

        // 速度
        byte[] speed = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int speedValue = bytesToUnsignedShort(speed);
        System.out.println("    Speed: " + speedValue + " cm/s");

        // 航向角
        byte[] heading = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int headingValue = bytesToUnsignedShort(heading);
        System.out.println("    Heading: " + headingValue + "°");

        // 长度
        byte[] lengthBytes = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int lengthValue = bytesToUnsignedShort(lengthBytes);
        System.out.println("    Length: " + lengthValue + " cm");

        // 宽度
        byte[] width = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int widthValue = bytesToUnsignedShort(width);
        System.out.println("    Width: " + widthValue + " cm");

        // 高度
        byte[] height = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int heightValue = bytesToUnsignedShort(height);
        System.out.println("    Height: " + heightValue + " cm");

        // X坐标
        byte[] x = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int xValue = bytesToShort(x);
        System.out.println("    X: " + xValue + " cm");

        // Y坐标
        byte[] y = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int yValue = bytesToShort(y);
        System.out.println("    Y: " + yValue + " cm");

        // Z坐标
        byte[] z = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int zValue = bytesToShort(z);
        System.out.println("    Z: " + zValue + " cm");

        // 跟踪击中次数
        byte[] trackCount = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        System.out.println("    Track Count: " + bytesToHex(trackCount));
    }

    private static void parseVideoInfo(byte[] content, int start) {
        int pos = start;

        // ID
        byte[] id = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        System.out.println("    ID: " + bytesToHex(id) + " (十进制: " + bytesToInt(id) + ")");

        // 类型
        byte type = content[pos++];
        String typeStr = type < FUSION_TYPES.length ? FUSION_TYPES[type] : "未知类型";
        System.out.println("    Type: " + type + " (" + typeStr + ")");

        // 置信度
        byte confidence = content[pos++];
        System.out.println("    Confidence: " + (confidence & 0xFF) + "%");

        // 颜色
        byte color = content[pos++];
        String colorStr = color < COLORS.length ? COLORS[color] : "未知颜色";
        System.out.println("    Color: " + color + " (" + colorStr + ")");

        // 信息来源
        byte source = content[pos++];
        String sourceStr = source < SOURCES.length ? SOURCES[source] : "未知来源";
        System.out.println("    Source: " + source + " (" + sourceStr + ")");

        // 符号位
        byte signBit = content[pos++];
        System.out.println("    Sign Bit: " + String.format("0x%02X", signBit));

        // 相机ID
        byte cameraId = content[pos++];
        String cameraStr = cameraId < CAMERA_IDS.length ? CAMERA_IDS[cameraId] : "未知相机";
        System.out.println("    Camera ID: " + cameraId + " (" + cameraStr + ")");

        // 经度
        byte[] longitude = Arrays.copyOfRange(content, pos, pos + 4);
        pos += 4;
        double longitudeValue = bytesToCoordinate(longitude);
        System.out.println("    Longitude: " + longitudeValue + "°");

        // 纬度
        byte[] latitude = Arrays.copyOfRange(content, pos, pos + 4);
        pos += 4;
        double latitudeValue = bytesToCoordinate(latitude);
        System.out.println("    Latitude: " + latitudeValue + "°");

        // 海拔
        byte[] altitude = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int altitudeValue = bytesToShort(altitude);
        System.out.println("    Altitude: " + altitudeValue + " cm");

        // 速度
        byte[] speed = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int speedValue = bytesToUnsignedShort(speed);
        System.out.println("    Speed: " + speedValue + " cm/s");

        // 航向角
        byte[] heading = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int headingValue = bytesToUnsignedShort(heading);
        System.out.println("    Heading: " + headingValue + "°");

        // 长度
        byte[] lengthBytes = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int lengthValue = bytesToUnsignedShort(lengthBytes);
        System.out.println("    Length: " + lengthValue + " cm");

        // 宽度
        byte[] width = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int widthValue = bytesToUnsignedShort(width);
        System.out.println("    Width: " + widthValue + " cm");

        // 高度
        byte[] height = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int heightValue = bytesToUnsignedShort(height);
        System.out.println("    Height: " + heightValue + " cm");

        // X坐标
        byte[] x = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int xValue = bytesToShort(x);
        System.out.println("    X: " + xValue + " cm");

        // Y坐标
        byte[] y = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int yValue = bytesToShort(y);
        System.out.println("    Y: " + yValue + " cm");

        // Z坐标
        byte[] z = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int zValue = bytesToShort(z);
        System.out.println("    Z: " + zValue + " cm");

        // 跟踪击中次数
        byte[] trackCount = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        System.out.println("    Track Count: " + bytesToHex(trackCount));
    }

    private static void parseDetectionBox(byte[] content, int start) {
        int pos = start;

        // 左上x
        byte[] topLeftX = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int x1 = bytesToShort(topLeftX);
        System.out.println("    Top Left X: " + x1 + " pixels");

        // 左上y
        byte[] topLeftY = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int y1 = bytesToShort(topLeftY);
        System.out.println("    Top Left Y: " + y1 + " pixels");

        // 右下x
        byte[] bottomRightX = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int x2 = bytesToShort(bottomRightX);
        System.out.println("    Bottom Right X: " + x2 + " pixels");

        // 右下y
        byte[] bottomRightY = Arrays.copyOfRange(content, pos, pos + 2);
        pos += 2;
        int y2 = bytesToShort(bottomRightY);
        System.out.println("    Bottom Right Y: " + y2 + " pixels");

        // 计算宽度和高度
        int width = x2 - x1;
        int height = y2 - y1;
        System.out.println("    Detection Box Size: " + width + "x" + height + " pixels");
    }

    // ========== 辅助转换方法 ==========

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }

    private static int bytesToInt(byte[] bytes) {
        int value = 0;
        for (byte b : bytes) {
            value = (value << 8) + (b & 0xFF);
        }
        return value;
    }

    private static long bytesToLong(byte[] bytes) {
        long value = 0;
        for (byte b : bytes) {
            value = (value << 8) + (b & 0xFF);
        }
        return value;
    }

    private static int bytesToShort(byte[] bytes) {
        return (short)((bytes[0] << 8) | (bytes[1] & 0xFF));
    }

    private static int bytesToUnsignedShort(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF);
    }

    private static long bytesToUnsignedInt(byte[] bytes) {
        long value = 0;
        for (byte b : bytes) {
            value = (value << 8) + (b & 0xFF);
        }
        return value;
    }

    private static double bytesToCoordinate(byte[] bytes) {
        // 将4字节转换为有符号整数
        int intValue = (bytes[0] << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                (bytes[3] & 0xFF);

        // 转换为度（假设原始数据是度*1e7的整数）
        return intValue / 10000000.0;
    }
}