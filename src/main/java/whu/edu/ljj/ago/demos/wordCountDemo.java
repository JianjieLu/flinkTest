package whu.edu.ljj.ago.demos;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.util.Collector;

import java.util.Arrays;
// whu.edu.ljj.stream.WordCountStreamUnboundedDemo
public class wordCountDemo {
    public static void main(String[] args) throws Exception {
// 设置 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 配置 KafkaSource
        String bootstrapServers = "192.168.0.5:9092"; // Kafka集群地址
        String topic = "test-topic"; // 要消费的topic
        String groupId = "flink-group"; // 消费者组ID

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest()) // 从最早的偏移量开始
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 反序列化消息为String
                .build();

// 从 KafkaSource 创建数据流
        DataStreamSource<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

// 处理数据：单词计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCounts = kafkaStream
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
// 按空格分割字符串，输出 (word, 1) 格式
                    Arrays.stream(value.split("\\s+"))
                            .forEach(word -> out.collect(Tuple2.of(word, 1)));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy(tuple -> tuple.f0) // 按单词分组
                .sum(1); // 按单词计数

// 打印结果到控制台
        wordCounts.print();

// 执行 Flink 作业
        env.execute("Flink Kafka Word Count");
    }
}