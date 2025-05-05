package whu.edu.ljj.flink.utils.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Properties;

public class NewsConsumer {
    public static void main(String[] args) {
        // Kafka 配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "100.65.38.40:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "news-consumer-group");  // 消费者组 ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // 从最早的消息开始消费

        // 创建 Kafka 消费者
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 订阅主题MergedPathData
//            consumer.subscribe(java.util.Collections.singletonList("MergedPathData.sceneTest.1"));
            consumer.subscribe(java.util.Collections.singletonList("MergedPathData"));
//            consumer.subscribe(java.util.Collections.singletonList("fiberDataTest1"));

            // 循环拉取消息
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);  // 每秒拉取一次消息

                // 处理拉取到的消息
                for (ConsumerRecord<String, String> record : records) {
//                    try (BufferedWriter writer1 = new BufferedWriter(new FileWriter("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\mergedata\\test.txt",true))) {
//                        writer1.write(record.value());
//                        writer1.write(System.lineSeparator());
//                    }
                    System.out.println(record);
//                        System.out.println("laqv...");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
