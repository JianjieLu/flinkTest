package whu.edu.ljj.ago.stream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

public class dealData1 {
    public static void main(String[] args) throws Exception {
        String topic = args[0]; // Kafka topic
        long timeSplit = Long.parseLong(args[1]); // 时间间隔（毫秒）

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
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        long startTime = 1737017338001L; // 起始时间，假设为固定值

        // 处理数据
        kafkaStream.flatMap((String jsonString, Collector<TimeRangeCarId> out) -> {
                    try {
                        JSONObject jsonObject = new JSONObject(jsonString);
                        String deviceip = jsonObject.getString("DEVICEIP");
                        long timeObs = jsonObject.getLong("TIME");
                        JSONArray tdataArray = jsonObject.getJSONArray("TDATA");

                        // 动态更新 startTime
                        long dynamicStartTime = (timeObs - startTime) / timeSplit;
                        for (int i = 0; i < tdataArray.length(); i++) {
                            JSONObject tdataObject = tdataArray.getJSONObject(i);
                            String carId = tdataObject.getString("Carnumber");

                            long rangeStart = startTime + dynamicStartTime * timeSplit;
                            long rangeEnd = rangeStart + timeSplit;
                            System.out.println("rangeStart" + rangeStart + "  " + "rangeEnd" + rangeEnd);

                            out.collect(new TimeRangeCarId(rangeStart, rangeEnd, carId));
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing JSON: " + e.getMessage());
                    }
                })
                .returns(TypeInformation.of(TimeRangeCarId.class)) // 显式指定返回类型
                .keyBy(TimeRangeCarId::getRangeStart)
                .process(new TimeRangeKeyedProcessFunction())
                .print();

        env.execute("Flink Kafka JSON Processing");
    }

    /**
     * 自定义类：用于存储时间范围和 carId
     */
    public static class TimeRangeCarId {
        private final long rangeStart;
        private final long rangeEnd;
        private final String carId;

        public TimeRangeCarId(long rangeStart, long rangeEnd, String carId) {
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            this.carId = carId;
        }

        public long getRangeStart() {
            return rangeStart;
        }

        public long getRangeEnd() {
            return rangeEnd;
        }

        public String getCarId() {
            return carId;
        }

        @Override
        public String toString() {
            return "Range: [" + rangeStart + " - " + rangeEnd + "], CarId: " + carId;
        }
    }

    /**
     * 自定义窗口处理逻辑
     */
    public static class TimeRangeKeyedProcessFunction extends KeyedProcessFunction<Long, TimeRangeCarId, String> {
    private final Set<String> uniqueCarIds = new HashSet<>();

    @Override
    public void processElement(TimeRangeCarId value, Context ctx, Collector<String> out) throws Exception {
        // 将当前元素的 carId 添加到去重集合
        uniqueCarIds.add(value.getCarId());

        // 注册定时器，设置触发时间为当前 rangeEnd 的时间
        long timerTimestamp = value.getRangeEnd();
        ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 输出结果，只在定时器触发时输出
        long rangeStart = ctx.getCurrentKey();
        out.collect("Key: " + rangeStart + " -> Unique CarIds: " + uniqueCarIds);

        // 清空集合，确保每个 key 只输出一次
        uniqueCarIds.clear();
    }
}
}
