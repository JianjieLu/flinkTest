package whu.edu.moniData.UDPRece;

import com.alibaba.fastjson2.JSON;
import lombok.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class test {

    public static void main(String[] args) {
        String jsonStr="{\"sequence\":0,\"mainCmd\":225,\"subCmd\":2,\"status\":0,\"msgLength\":302,\"deviceId\":14,\"reserved1\":0,\"frameNum\":0,\"timestampMicrosec\":1749704865512,\"longitude\":30.9201185,\"latitude\":30.9201185,\"angle\":0.0,\"participantCount\":2,\"reserved2\":34721,\"participants\":[{\"id\":12037,\"type\":202,\"confidence\":0.0,\"color\":0,\"source\":2,\"signBit\":3,\"cameraId\":0,\"longitude\":114.0450444,\"latitude\":30.9202558,\"altitude\":0.0,\"speed\":0.03,\"heading\":1.48,\"length\":9.21,\"width\":2.6,\"height\":2.49,\"X\":21.95,\"Y\":5.83,\"Z\":-5.89,\"trackCount\":0},{\"id\":12054,\"type\":0,\"confidence\":0.0,\"color\":0,\"source\":2,\"signBit\":2,\"cameraId\":0,\"longitude\":114.0452275,\"latitude\":30.9200632,\"altitude\":0.0,\"speed\":19.08,\"heading\":3.25,\"length\":4.35,\"width\":1.8,\"height\":1.82,\"X\":5.5,\"Y\":2.8,\"Z\":-4.46,\"trackCount\":0}],\"frontCameraTs\":0,\"bodyCameraTs\":0,\"rearCameraTs\":0,\"reserved3\":0,\"checksum\":224}";
        UDPData data = JSON.parseObject(jsonStr, UDPData.class);
        System.out.println(data);
    }

    // 文档要求的UDPData格式
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
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
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
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

        String plateNo="";

    }
}