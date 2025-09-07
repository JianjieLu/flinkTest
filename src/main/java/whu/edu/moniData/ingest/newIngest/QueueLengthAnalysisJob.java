package whu.edu.moniData.ingest.newIngest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 排队长度计算作业
 * 处理MergedPathData数据，计算AK1路段的排队长度
 * 每5秒处理一条数据
 */
public class QueueLengthAnalysisJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度，根据实际情况调整
        env.setParallelism(1);

        // ================== Kafka 配置 ==================
        String brokers = "10.48.53.82:9092"; // Kafka broker地址
        String inputTopic = "MergedPathData"; // 输入主题
        String outputTopic = "QueueLengthOutput"; // 输出主题
        String groupId = "queue-length-group"; // 消费者组ID

        // ================== 创建Kafka数据源 ==================
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(Collections.singletonList(inputTopic))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 从Kafka读取数据流
        DataStream<String> inputStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // ================== 处理数据流 ==================
        // 使用5秒滚动窗口，每5秒处理一条数据
        SingleOutputStreamOperator<String> processedStream = inputStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowQueueLengthCalculator())
                .name("Queue Length Calculator");

        // ================== 创建Kafka输出Sink ==================
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // ================== 输出结果 ==================
        processedStream.sinkTo(sink).name("Queue Length Output");

        // ================== 执行作业 ==================
        env.execute("Queue Length Analysis Job (5s Sampling)");
    }

    /**
     * 窗口排队长度计算器
     * 每5秒处理一条数据，计算AK1路段的排队长度
     */
    private static class WindowQueueLengthCalculator implements AllWindowFunction<String, String, TimeWindow> {
        // 正则表达式匹配AK1桩号格式
        private static final Pattern STAKE_PATTERN = Pattern.compile("AK1\\+(\\d+)");

        @Override
        public void apply(TimeWindow window, Iterable<String> values, Collector<String> out) throws Exception {
            // 只处理窗口中的第一条数据
            for (String jsonString : values) {
                try {
                    JSONObject jsonObject = new JSONObject(jsonString);
                    String timestamp = jsonObject.getString("timeStamp");
                    JSONArray pathList = jsonObject.getJSONArray("pathList");

                    // 过滤出AK1路段的车辆
                    List<VehicleData> ak1Vehicles = new ArrayList<>();

                    for (int i = 0; i < pathList.length(); i++) {
                        JSONObject vehicle = pathList.getJSONObject(i);
                        String stakeId = vehicle.optString("stakeId", "");

                        // 检查是否为AK1路段
                        Matcher matcher = STAKE_PATTERN.matcher(stakeId);
                        if (matcher.find()) {
                            try {
                                double stakeNum = Double.parseDouble(matcher.group(1));
                                // 只处理0到223之间的车辆
                                if (stakeNum >= 0 && stakeNum <= 223) {
                                    double speed = vehicle.optDouble("speed", 0);
                                    ak1Vehicles.add(new VehicleData(stakeNum, speed));
                                }
                            } catch (NumberFormatException e) {
                                // 忽略格式错误的桩号
                                System.err.println("Invalid stake number format: " + stakeId);
                            }
                        }
                    }

                    // 计算排队长度
                    double queueLength = calculateQueueLength(ak1Vehicles);

                    // 创建输出JSON
                    JSONObject output = new JSONObject();
                    output.put("timestamp", timestamp);
                    output.put("windowStart", window.getStart());
                    output.put("windowEnd", window.getEnd());
                    output.put("queueLength", queueLength);
                    output.put("vehicleCount", ak1Vehicles.size());
                    output.put("isQueue", queueLength > 0);

                    // 收集结果
                    out.collect(output.toString());

                    // 只处理第一条数据，然后退出循环
                    break;

                } catch (Exception e) {
                    System.err.println("Error processing message: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        /**
         * 计算排队长度
         * @param vehicles AK1路段的车辆列表
         * @return 排队长度（米），如果没有排队则返回0
         */
        private double calculateQueueLength(List<VehicleData> vehicles) {
            if (vehicles.isEmpty()) {
                return 0.0;
            }

            // 按桩号降序排序（从大到小）
            vehicles.sort((v1, v2) -> Double.compare(v2.stakeNum, v1.stakeNum));

            // 检查最接近收费站的车辆速度是否低于5km/h
            if (vehicles.get(0).speed >= 5.0) {
                return 0.0;
            }

            // 查找排队队列
            List<VehicleData> queue = new ArrayList<>();
            queue.add(vehicles.get(0));

            for (int i = 1; i < vehicles.size(); i++) {
                VehicleData current = vehicles.get(i);
                VehicleData lastInQueue = queue.get(queue.size() - 1);

                // 检查间距是否小于10米且速度低于5km/h
                double distance = lastInQueue.stakeNum - current.stakeNum;
                if (distance < 10.0 && current.speed < 5.0) {
                    queue.add(current);
                } else {
                    break;
                }
            }

            // 计算排队长度
            double endStake = queue.get(queue.size() - 1).stakeNum;
            return 223.0 - endStake;
        }

        /**
         * 车辆数据内部类
         */
        private static class VehicleData {
            double stakeNum; // 桩号数值
            double speed;    // 速度(km/h)

            VehicleData(double stakeNum, double speed) {
                this.stakeNum = stakeNum;
                this.speed = speed;
            }
        }
    }
}