package com.data;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.json.JSONObject;
import org.json.JSONArray;
import org.zeromq.ZMsg;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

public class ZmqSource implements SourceFunction<JSONObject> {
    private final String address;
    private volatile boolean isRunning = true;
    private ZContext context;
    private ZMQ.Socket socket;

    public ZmqSource(String address) {
        this.address = address;
    }

    @Override
    public void run(SourceContext<JSONObject> ctx) throws Exception {
        context = new ZContext();
        socket = context.createSocket(SocketType.SUB);
        socket.connect(address);
        socket.subscribe("".getBytes()); // 订阅所有消息

        while (isRunning) {
            ZMsg msg = ZMsg.recvMsg(socket);

            if (msg != null) {
                String line = msg.toString();

                line = line.replaceAll("[\\[\\]]", "").trim();

                JSONObject TrajePoints = decodeRecord(line);

                msg.destroy(); // 释放消息资源

                ctx.collect(TrajePoints); // 将数据发送到 Flink 流中
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        if (context != null) {
            context.close();
        }
    }

    private static JSONObject decodeRecord(String hex) {
        JSONObject TrajePoints = new JSONObject();

        byte[] data = hexStringToByteArray(hex);

        // 解析SN, DEVICEIP, TIME, COUNT
        int sn = byteArrayToIntLittleEndian(data, 0);
        String deviceIp = byteArrayToIp(data, 4);
        long time = byteArrayToLongLittleEndian(data, 8);
        int count = byteArrayToIntLittleEndian(data, 16);

        TrajePoints.put("SN", sn);
        TrajePoints.put("DEVICEIP", deviceIp);
        TrajePoints.put("TIME", time);
        TrajePoints.put("COUNT", count);

        JSONArray TDATAList = new JSONArray();
        // 解析车辆信息
        for (int i = 0; i < count; i++) {
            int offset = 20 + i * 44; // 每个TDATA占44字节
            TDATAList.put(decodeVehicleData(data, offset));
        }

        TrajePoints.put("TDATA", TDATAList);
        return TrajePoints;
    }

    private static JSONObject decodeVehicleData(byte[] data, int offset) {
        long carId = byteArrayToLongLittleEndian(data, offset);
        String carNumber = new String(data, offset + 8, 16, StandardCharsets.UTF_8).trim();
        byte type = data[offset + 24];
        int[] scope = {byteArrayToIntLittleEndian(data, offset + 25), byteArrayToIntLittleEndian(data, offset + 29)};
        float speed = Float.intBitsToFloat(byteArrayToIntLittleEndian(data, offset + 33));
        byte wayno = data[offset + 37];
        int tpointno = byteArrayToIntLittleEndian(data, offset + 38);
        byte booleanValue = data[offset + 42];
        byte direct = data[offset + 43];

        JSONObject TDATAPoint = new JSONObject();
        TDATAPoint.put("ID", carId);
        TDATAPoint.put("Carnumber", carNumber);
        TDATAPoint.put("Type", type);
        TDATAPoint.put("Scope", new JSONArray(Arrays.stream(scope).boxed().collect(Collectors.toList())));
        TDATAPoint.put("Speed", speed);
        TDATAPoint.put("Wayno", wayno);
        TDATAPoint.put("Tpointno", tpointno);
        TDATAPoint.put("Boolean", booleanValue);
        TDATAPoint.put("Direct", direct);

        return TDATAPoint;
    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private static String byteArrayToIp(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) + "." +
                (bytes[offset + 1] & 0xFF) + "." +
                (bytes[offset + 2] & 0xFF) + "." +
                (bytes[offset + 3] & 0xFF));
    }

    private static int byteArrayToIntLittleEndian(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) |
                ((bytes[offset + 1] & 0xFF) << 8) |
                ((bytes[offset + 2] & 0xFF) << 16) |
                ((bytes[offset + 3] & 0xFF) << 24));
    }

    private static long byteArrayToLongLittleEndian(byte[] bytes, int offset) {
        return ((long) bytes[offset] & 0xFF) |
                ((long) bytes[offset + 1] & 0xFF) << 8 |
                ((long) bytes[offset + 2] & 0xFF) << 16 |
                ((long) bytes[offset + 3] & 0xFF) << 24 |
                ((long) bytes[offset + 4] & 0xFF) << 32 |
                ((long) bytes[offset + 5] & 0xFF) << 40 |
                ((long) bytes[offset + 6] & 0xFF) << 48 |
                ((long) bytes[offset + 7] & 0xFF) << 56;
    }
}
