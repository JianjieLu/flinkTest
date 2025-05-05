package whu.edu.ljj.ago.stream;

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
        if (args.length < 2) {
            System.err.println("Usage: <topic> <timesplit>");
            return;
        }

        String topic = args[0]; // Kafka topic
        long timesplit = Long.parseLong(args[1]); // 时间分片大小

        // 设置 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 KafkaSource
        String bootstrapServers = "192.168.0.5:9092"; // Kafka集群地址
        String groupId = "flink-group"; // 消费者组ID

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 从 KafkaSource 创建数据流
        DataStreamSource<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        kafkaStream
            .flatMap(new FlatMapFunction<String, Tuple3<String, HashSet<String>, Integer>>() {
                long finalStartTime = 1737017338001L;
                long fixedStartTime = 1737017338001L;

                @Override
                public void flatMap(String jsonString, Collector<Tuple3<String, HashSet<String>, Integer>> out) throws Exception {
                    try {
                        JSONObject jsonObject = new JSONObject(jsonString);
                        long timeobs = jsonObject.getLong("TIME");
                        String deviceIp = jsonObject.getString("DEVICEIP");
                        long starttime = calRange(timeobs, timesplit, fixedStartTime);

                        String timeSeg = starttime + "-" + (starttime + timesplit) + "-" + getLastNString(deviceIp, 1);

                        JSONArray tdataArray = jsonObject.getJSONArray("TDATA");
                        HashSet<String> carNumbers = new HashSet<>();
                        for (int i = 0; i < tdataArray.length(); i++) {
                            JSONObject tdataObject = tdataArray.getJSONObject(i);
                            carNumbers.add(tdataObject.getString("Carnumber"));
                        }
                        if(timeobs>finalStartTime){
                            finalStartTime=finalStartTime+timesplit;
                            out.collect(new Tuple3<>(timeSeg, carNumbers, carNumbers.size()));
                        }
                    } catch (Exception e) {
                        // 忽略解析错误
                    }
                }
            })
            .keyBy(tuple -> tuple.f0)
            .process(new KeyedProcessFunction<String, Tuple3<String, HashSet<String>, Integer>, String>() {
                @Override
                public void processElement(Tuple3<String, HashSet<String>, Integer> value, Context ctx, Collector<String> out) throws Exception {
                    out.collect("Key: " + value.f0 + ", Data: (" +
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

    public static String getLastNString(String input, int num) {
        return input.substring(input.length() - num);
    }
}
