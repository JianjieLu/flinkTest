package whu.edu.ljj.ago.demos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class readKafkaDemo {

    public static void main(String[] args) throws Exception {
        String topic = "fiberDataTest2";//topic

        // 创建 Flink 的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka 配置
        String bootstrapServers = "100.65.38.139:9092";
        String groupId = "flink-group";

        // 创建 KafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest()) // 从最新位置开始消费
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 使用简单字符串反序列化器
                .build();

        // 从 Kafka 读取数据流
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(), // 无水印策略
                "Kafka Source"
        );

        // 处理数据：将数据转换为大写并打印
        kafkaStream
                .print(); // 打印到控制台

        // 启动 Flink 作业
        env.execute("Flink Kafka Consumer Example");
    }
}
