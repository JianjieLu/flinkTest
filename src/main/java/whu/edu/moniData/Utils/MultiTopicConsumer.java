package whu.edu.moniData.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MultiTopicConsumer {
    public static void main(String[] args) {
        // 1. 创建消费者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "100.65.38.139:9092");  // 修正参数名
        props.put("group.id", "my-consuming-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        // 2. 创建消费者实例
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // 3. 订阅多个Topic
//            consumer.subscribe(Arrays.asList("e1_data_XG01"));
            consumer.subscribe(Arrays.asList("fiberDataTest1"));
//            consumer.subscribe(Arrays.asList("MergedPathData.sceneTest.1"));

            // 4. 持续轮询消息
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));  // 使用新版API

                // 5. 处理每条消息
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("\n收到消息: \n"
                                    + "Topic: %s\n"
                                    + "Partition: %d\n"
                                    + "Offset: %d\n"
                                    + "Key: %s\n"
                                    + "Value: %s\n",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value());
                }
            }
        }
    }
}