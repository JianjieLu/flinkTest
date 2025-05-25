package whu.edu.moniData.Utils;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class ProtocolReceiver {
    private static final int PORT = 7171; // 替换为实际端口号

    public static void main(String[] args) {
        try (DatagramSocket socket = new DatagramSocket(PORT)) {
            byte[] buffer = new byte[2048]; // 根据最大报文长度调整
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            while (true) {
                socket.receive(packet);
                byte[] data = Arrays.copyOfRange(packet.getData(), 0, packet.getLength());
                parseProtocolData(data);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void parseTunnelSceneData(byte[] content) {
        ByteBuffer buffer = ByteBuffer.wrap(content).order(ByteOrder.BIG_ENDIAN);

        // 设备ID（2字节）
        int deviceId = buffer.getShort() & 0xFFFF;

        // 预留位（2字节）
        buffer.getShort();

        // 点云数据帧号（4字节）
        long frameNumber = buffer.getInt() & 0xFFFFFFFFL;

        // 时间戳秒（4字节）
        long timestampSec = buffer.getInt() & 0xFFFFFFFFL;

        // 时间戳微秒（4字节）
        long timestampMicro = buffer.getInt() & 0xFFFFFFFFL;

        // 激光器原点经度、纬度（各4字节）
        long longitude = buffer.getInt();
        long latitude = buffer.getInt();

        // 激光器角度（2字节）
        int angle = buffer.getShort() & 0xFFFF;

        // 交通参与者数量（1字节）
        int participantCount = buffer.get() & 0xFF;

        // 预留（5字节）
        buffer.get(new byte[5]);

        // 遍历每个交通参与者
        for (int i = 0; i < participantCount; i++) {
            parseParticipant(buffer);
        }

        // 车头/车身/车尾相机时间戳（各8字节）
        long frontCameraTs = buffer.getLong();
        long bodyCameraTs = buffer.getLong();
        long rearCameraTs = buffer.getLong();

        // 预留（4字节）
        buffer.get(new byte[4]);
    }

    private static void parseParticipant(ByteBuffer buffer) {
        // 解析融合数据（示例）
        int id = buffer.getShort() & 0xFFFF;
        int type = buffer.get() & 0xFF;
        int confidence = buffer.get() & 0xFF;
        int color = buffer.get() & 0xFF;
        int source = buffer.get() & 0xFF;
        int signBit = buffer.get() & 0xFF;
        int cameraId = buffer.get() & 0xFF;

        // 经度、纬度（各4字节）
        int longitude = buffer.getInt();
        int latitude = buffer.getInt();

        // 海拔（2字节）
        short altitude = buffer.getShort();

        // 其他字段（速度、航向角等）类似...
        // 根据协议继续解析剩余字段
    }
    private static void parseProtocolData(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);

        // 1. 开始位（2字节，固定0xFFFF）
        short start = buffer.getShort();
        if (start != (short) 0xFFFF) {
            throw new IllegalArgumentException("Invalid start bytes");
        }

        // 2. 序列号（1字节）
        int sequence = buffer.get() & 0xFF;

        // 3. 主命令号（1字节）
        int mainCmd = buffer.get() & 0xFF;
        if (mainCmd != 0xE1) {
            throw new IllegalArgumentException("Invalid main command");
        }

        // 4. 子命令号（1字节）
        int subCmd = buffer.get() & 0xFF;

        // 5. 状态位（1字节）
        int status = buffer.get() & 0xFF;

        // 6. 消息长度（2字节）
        int msgLength = buffer.getShort() & 0xFFFF;

        // 7. 消息内容（N字节）
        byte[] msgContent = new byte[msgLength];
        buffer.get(msgContent);

        // 8. 校验位（1字节）
        int checksum = buffer.get() & 0xFF;

        // 9. 截止位（1字节，固定0xFF）
        byte end = buffer.get();
        if (end != (byte) 0xFF) {
            throw new IllegalArgumentException("Invalid end byte");
        }

        // 根据子命令号解析消息内容
        if (subCmd == 0x03) {
            parseTunnelSceneData(msgContent);
        }
    }

}