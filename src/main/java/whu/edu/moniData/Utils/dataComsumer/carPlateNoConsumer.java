package whu.edu.moniData.Utils.dataComsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Properties;

public class carPlateNoConsumer {
    // 定义时间格式解析器（线程安全）
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    public static void main(String[] args) {
        // 1. 创建消费者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.48.53.82:9092");
        props.put("group.id", "my-consuming-group1");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "latest");

        // 2. 创建消费者实例
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 3. 订阅Topic
            consumer.subscribe(Arrays.asList("jtkj.jga.path"));

            // 4. 持续轮询消息
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                // 5. 处理每条消息
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JSONObject jsonObj = new JSONObject(record.value());

                        // 检查消息中是否包含pathList
                        if (jsonObj.has("pathList")) {
                            JSONArray pathList = jsonObj.getJSONArray("pathList");

                            // 遍历pathList，查找plateNo为"浙A12345"的车辆数据
                            for (int i = 0; i < pathList.length(); i++) {
                                JSONObject pathItem = pathList.getJSONObject(i);

                                if (pathItem.has("plateNo") &&
                                        100<pathItem.getDouble("speed")) {

                                    // 找到目标车辆，输出完整消息
                                    System.out.printf("\n=== 找到目标车辆 冀A8IA55 ===\n"
                                                    + "Topic: %s\n"
                                                    + "Partition: %d\n"
                                                    + "Offset: %d\n"
//                                                    + "Key: %s\n"
//                                                    + "完整数据: %s\n"
                                                    + "目标车辆数据: %s\n",
                                            record.topic(),
                                            record.partition(),
                                            record.offset(),
//                                            record.key(),
//                                            record.value(),
                                            pathItem.getDouble("speed")); // 格式化输出目标车辆数据

                                    // 如果需要只输出目标车辆的具体信息，可以这样：
                                    System.out.println("\n目标车辆详细信息:");
                                    System.out.println("车牌号: " + pathItem.getString("plateNo"));
                                    if (pathItem.has("timestamp")) {
                                        System.out.println("时间戳: " + pathItem.getString("timestamp"));
                                    }
                                    if (pathItem.has("longitude")) {
                                        System.out.println("经度: " + pathItem.getDouble("longitude"));
                                    }
                                    if (pathItem.has("latitude")) {
                                        System.out.println("纬度: " + pathItem.getDouble("latitude"));
                                    }
                                    // 可以根据实际数据结构添加更多字段
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("解析消息时发生错误: " + e.getMessage());
                        System.err.println("原始消息: " + record.value());
                    }
                }
            }
        }
    }
}