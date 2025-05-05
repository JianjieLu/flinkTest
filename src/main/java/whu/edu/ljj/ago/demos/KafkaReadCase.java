package whu.edu.ljj.ago.demos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaReadCase {

    public static void main(String[] args) throws Exception {
        // 从程序参数中获取 Kafka Topic
        String topic = args.length > 0 ? args[0] : "topic1"; // 如果没有提供参数，使用默认 topic

        // 创建 Flink 的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka 配置
        String bootstrapServers = "192.168.0.5:9092"; // Kafka 服务器地址
        String groupId = "flink-group"; // 消费者组 ID

        // 创建 KafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers) // 设置 Kafka 服务器
                .setTopics(topic) // 设置消费的 Topic
                .setGroupId(groupId) // 设置消费者组 ID
                .setStartingOffsets(OffsetsInitializer.earliest()) // 从最新的偏移量开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 反序列化消息为字符串
                .build();

        // 从 Kafka 中读取数据流
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(), // 不需要水印策略
                "Kafka Source" // 数据流名称
        );

        // 数据处理：将消息转换为大写并输出
        kafkaStream

                .print(); // 打印到控制台

        // 启动 Flink 作业
        env.execute("Kafka Uppercase Transformation");
    }
}
