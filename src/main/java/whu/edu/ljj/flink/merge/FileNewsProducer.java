package whu.edu.ljj.flink.merge;

import org.apache.kafka.clients.producer.*;
import java.io.*;
import java.util.Properties;

class FileNewsProducer {
    public static void main(String[] args) throws InterruptedException, IOException {
        // Kafka配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "100.65.38.40:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);  // 失败重试机制

        // 文件路径（假设文件包含新闻内容，每行一条）
        String filePath = "D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\mergedata\\yuanban.txt"; // 请替换为你的文件路径

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath));
             Producer<String, String> producer = new KafkaProducer<>(props)) {

            String newsLine;
            int i = 1;
            while ((newsLine = reader.readLine()) != null) {
                // 假设每行是新闻内容，构造Kafka消息

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("news-topics", "key-" + i, newsLine);

                // 异步发送带回调
                producer.send(record, (metadata, e) -> {
                    if (e == null) {
                        System.out.printf("已发送新闻至 [分区%d][Offset%d]\n",
                                metadata.partition(), metadata.offset());
                    } else {
                        System.err.println("发送失败: " + e.getMessage());
                    }
                });
//                JSON解析失败: {"timeStamp":"2025-03-23 18:18:42:692","pathNum":99,"pathList":[],"time":1742725122872,"waySectionId":"DBDE2B10-7E71-493D-8FE7-16AC5A9891C2","waySectionName":"京港澳高速湖北北段"
                i++;
                Thread.sleep(200); // 每秒发送一条
            }
        }
    }
}
