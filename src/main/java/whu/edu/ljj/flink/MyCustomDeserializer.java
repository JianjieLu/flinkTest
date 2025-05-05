package whu.edu.ljj.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MyCustomDeserializer implements DeserializationSchema<String> {

    @Override
    public String deserialize(byte[] message) throws IOException {
        String value = new String(message, StandardCharsets.UTF_8);
        // 这里可以直接返回消息内容，也可以在其他地方保存 topic 信息
        System.out.println("Received message: " + value);
        return value;
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}