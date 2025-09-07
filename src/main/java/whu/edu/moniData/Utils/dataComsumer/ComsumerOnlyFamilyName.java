package whu.edu.moniData.Utils.dataComsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class ComsumerOnlyFamilyName {
    // 用于跟踪已看到的字段组合
    private static final Set<String> seenFieldCombinations = new HashSet<>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.48.53.82:9092");
        props.put("group.id", "field-inspector-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "latest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(
                    "completed.pathdata"
                    // 添加您感兴趣的其他主题
                    // "MergedPathData.sceneTest.1",
                    // "MergedPathData.sceneTest.2"
            ));

            System.out.println("开始监听主题，只显示JSON字段结构...");
            System.out.println("按 Ctrl+C 退出");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JSONObject json = new JSONObject(record.value());
                        String fieldStructure = getFieldStructure(json);

                        // 只打印新的字段组合
                        if (seenFieldCombinations.add(fieldStructure)) {
                            System.out.println("\n=== 发现新的字段结构 ===");
                            System.out.println("主题: " + record.topic());
                            System.out.println("字段结构: " + fieldStructure);
                            System.out.println("示例值: " + getSampleValues(json));
                        }
                    } catch (Exception e) {
                        // 非JSON消息处理
                        System.out.printf("\n非JSON消息 [主题: %s, 分区: %d]: %s%n",
                                record.topic(), record.partition(),
                                record.value().substring(0, Math.min(100, record.value().length())));
                    }
                }
            }
        }
    }

    // 获取字段结构字符串
    private static String getFieldStructure(JSONObject json) {
        return String.join(", ", json.keySet());
    }

    // 获取字段示例值
    private static String getSampleValues(JSONObject json) {
        StringBuilder sb = new StringBuilder("{");
        for (String key : json.keySet()) {
            Object value = json.get(key);
            String sample = value.toString();
            if (sample.length() > 20) {
                sample = sample.substring(0, 17) + "...";
            }
            sb.append(key).append(": ").append(sample).append(", ");
        }
        if (sb.length() > 1) sb.setLength(sb.length() - 2);
        return sb.append("}").toString();
    }
}