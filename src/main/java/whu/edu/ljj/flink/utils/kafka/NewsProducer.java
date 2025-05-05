package whu.edu.ljj.flink.utils.kafka;


import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class NewsProducer {
    public static void main(String[] args) throws InterruptedException {
        // Kafka配置（网页1、网页4）
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "100.65.38.40:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);  // 失败重试机制（网页4）

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // 循环发送10条新闻（网页4异步发送模式）
            for (int i = 1; i <= 100000; i++) {
                String news = "新闻" + i + "：今日头条消息...";

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("topic1", "key-" + i, news);

                // 异步发送带回调（网页4第三种方式）
                producer.send(record, (metadata, e) -> {
                    if (e == null) {
                        System.out.printf("已发送新闻至 [分区%d][Offset%d]\n",
                                metadata.partition(), metadata.offset());
                    } else {
                        System.err.println("发送失败: " + e.getMessage());
                    }
                });

                Thread.sleep(300); // 每秒发送一条（网页4同步发送示例的间隔逻辑）
            }
        }
    }
}