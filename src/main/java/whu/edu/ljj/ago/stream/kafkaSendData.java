package whu.edu.ljj.ago.stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;
//whu.edu.ljj.stream.kafkaDemo
public class kafkaSendData {
    public static void main(String[] args) {
//        String topic = args[0];//topic
        String topic = "topic1";//topic
        // Kafka集群的advertised.listeners地址
        String bootstrapServers = "192.168.0.5:9092";
        // 要发送消息的topic

        // 创建Kafka生产者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers); // Kafka集群地址
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // key的序列化器
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // value的序列化器

        // 创建Kafka生产者
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 创建Scanner对象读取控制台输入
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入消息（输入'exit'退出）：");

        try {
            while (true) {
                String message = scanner.nextLine();
                if ("exit".equalsIgnoreCase(message)) {
                    break; // 如果输入'exit'，则退出循环
                }

                // 创建ProducerRecord对象，指定topic和value
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);

                // 发送消息
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        // 发送失败，打印异常信息
                        exception.printStackTrace();
                    } else {
                        // 发送成功，打印消息的元数据信息
                        System.out.println("Message sent successfully. Topic: " + metadata.topic() + ", Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
                    }
                });
            }
        } finally {
            // 关闭生产者
            producer.close();
            // 关闭Scanner
            scanner.close();
        }
    }
}