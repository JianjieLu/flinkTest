package whu.edu.ljj.ago.oks;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashSet;

public class Demo {
    public static void main(String[] args) throws Exception {
        String topic = args[0]; // Kafka topic
        long timesplit = Long.parseLong(args[1]); // Kafka topic

        // 设置 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 KafkaSource
        String bootstrapServers = "192.168.0.5:9092"; // Kafka集群地址
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

        // 处理数据
        kafkaStream
            .flatMap(new FlatMapFunction<String, Tuple3<String, HashSet<String>, Integer>>() {
                @Override
                public void flatMap(String jsonString, Collector<Tuple3<String, HashSet<String>, Integer>> out) throws Exception {
                    try {
                        long startTime = 1737017338001L;
                        // 解析 JSON 数据
                        JSONObject jsonObject = new JSONObject(jsonString);
                        long timeobs = jsonObject.getLong("TIME");
                        String deviceIp = jsonObject.getString("DEVICEIP");
                        long starttime = calRange(timeobs, timesplit, startTime);
                        starttime=getEight(starttime);//实际要删除
                        String timeSeg = starttime + "-"+(starttime+timesplit) + "-"+getLastNString(deviceIp,1);
                        // 提取车牌号列表
                        JSONArray tdataArray = jsonObject.getJSONArray("TDATA");
                        HashSet<String> carNumbers = new HashSet<>();
                        for (int i = 0; i < tdataArray.length(); i++) {
                            JSONObject tdataObject = tdataArray.getJSONObject(i);
                            carNumbers.add(tdataObject.getString("Carnumber"));
                        }

                        // 将结果发送到下游
                        out.collect(new Tuple3<>(timeSeg, carNumbers, carNumbers.size()));
                    } catch (Exception e) {
                        // 输入不是有效 JSON 时忽略
                        System.err.println("解析 JSON 时出错: " + e.getMessage());
                    }
                }
            })
            .keyBy(tuple -> tuple.f0) // 按 timeSeg 分组
            .process(new KeyedProcessFunction<String, Tuple3<String, HashSet<String>, Integer>, String>() {
                @Override
                public void processElement(Tuple3<String, HashSet<String>, Integer> value, Context ctx, Collector<String> out) throws Exception {
                    // 输出当前记录的聚合结果
                    out.collect("Key: " + value.f0 + ", Data: ("  +
                            value.f1.toString() + ", " + value.f2 + ")");
                }
            })
            .print();

        // 执行 Flink 作业
        env.execute("Flink Kafka JSON Processing");
    }

    public static long calRange(long timeobs, long timesplit, long starttime) {
        long index = (timeobs - starttime) / timesplit;
        return starttime + timesplit * index;
    }
       public static long getEight(long input) {
        String numStr = String.valueOf(input);
        return Long.parseLong(numStr.substring(numStr.length() - 8));
    }
      public static String getLastNString(String input,int num) {
        return input.substring(input.length() - num);
    }
}
