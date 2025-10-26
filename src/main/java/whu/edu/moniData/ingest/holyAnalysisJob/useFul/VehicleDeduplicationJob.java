package whu.edu.moniData.ingest.holyAnalysisJob.useFul;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class VehicleDeduplicationJob {

    // 使用Flink状态管理的去重处理器
    public static class VehicleDeduplicationProcessor extends RichFlatMapFunction<String, String> {

        // 使用Flink的Keyed State来存储已处理的车辆ID
        private transient ValueState<Boolean> vehicleSeenState;

        // 用于存储车辆信息的临时状态
        private transient ValueState<String> vehicleInfoState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 初始化车辆是否已见的状态
            ValueStateDescriptor<Boolean> seenDescriptor =
                    new ValueStateDescriptor<>("vehicleSeenState", Boolean.class, false);
            vehicleSeenState = getRuntimeContext().getState(seenDescriptor);

            // 初始化车辆信息状态
            ValueStateDescriptor<String> infoDescriptor =
                    new ValueStateDescriptor<>("vehicleInfoState", String.class);
            vehicleInfoState = getRuntimeContext().getState(infoDescriptor);
        }

        @Override
        public void flatMap(String jsonString, Collector<String> out) throws Exception {
            JSONObject jsonObject = new JSONObject(jsonString);
            JSONArray pathList = jsonObject.getJSONArray("pathList");

            for (int i = 0; i < pathList.length(); i++) {
                JSONObject vehicleData = pathList.getJSONObject(i);
                String vehicleId = String.valueOf(vehicleData.getLong("id"));
                String plateNo = vehicleData.getString("plateNo");

                // 检查车辆是否已经处理过
                Boolean hasSeen = vehicleSeenState.value();

                if (hasSeen == null || !hasSeen) {
                    // 新车，输出并更新状态
                    JSONObject output = new JSONObject();
                    output.put("eventType", "NEW_VEHICLE");
                    output.put("vehicleId", vehicleId);
                    output.put("plateNo", plateNo);
                    output.put("timestamp", jsonObject.getString("timeStamp"));
                    output.put("longitude", vehicleData.getDouble("longitude"));
                    output.put("latitude", vehicleData.getDouble("latitude"));
                    output.put("vehicleType", vehicleData.getInt("originalType"));
                    output.put("speed", vehicleData.getDouble("speed"));

                    out.collect(output.toString());

                    // 更新状态，标记车辆已处理
                    vehicleSeenState.update(true);
                    vehicleInfoState.update(jsonString);
                }
            }
        }
    }

    // 基于内存的去重处理器（适合数据量不大的情况）
    public static class InMemoryVehicleDeduplicationProcessor extends RichFlatMapFunction<String, String> {

        private transient Set<String> processedVehicles;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            processedVehicles = ConcurrentHashMap.newKeySet();
        }

        @Override
        public void flatMap(String jsonString, Collector<String> out) throws Exception {
            JSONObject jsonObject = new JSONObject(jsonString);
            JSONArray pathList = jsonObject.getJSONArray("pathList");

            for (int i = 0; i < pathList.length(); i++) {
                JSONObject vehicleData = pathList.getJSONObject(i);
                String vehicleId = String.valueOf(vehicleData.getLong("id"));
                String plateNo = vehicleData.getString("plateNo");

                // 使用车辆ID作为去重键
                String dedupKey = vehicleId + "_" + plateNo;

                // 如果是新车
                if (!processedVehicles.contains(dedupKey)) {
                    // 输出新车信息
                    JSONObject output = new JSONObject();
                    output.put("eventType", "NEW_VEHICLE");
                    output.put("vehicleId", vehicleId);
                    output.put("plateNo", plateNo);
                    output.put("firstSeenTime", jsonObject.getString("timeStamp"));
                    output.put("longitude", vehicleData.getDouble("longitude"));
                    output.put("latitude", vehicleData.getDouble("latitude"));
                    output.put("laneNo", vehicleData.getInt("laneNo"));
                    output.put("vehicleType", vehicleData.getInt("originalType"));
                    output.put("direction", vehicleData.optInt("direction", -1));
                    output.put("speed", vehicleData.getDouble("speed"));

                    // 添加车辆颜色和重量（如果存在）
                    if (vehicleData.has("vehicleColor")) {
                        output.put("vehicleColor", vehicleData.getInt("vehicleColor"));
                    }
                    if (vehicleData.has("vehicleWeight")) {
                        output.put("vehicleWeight", vehicleData.getDouble("vehicleWeight"));
                    }
                    if (vehicleData.has("specialFlag")) {
                        output.put("specialFlag", vehicleData.getString("specialFlag"));
                    }

                    out.collect(output.toString());

                    // 添加到已处理集合
                    processedVehicles.add(dedupKey);

                    System.out.println("New vehicle detected: " + plateNo + " (ID: " + vehicleId + ")");
                }
            }
        }
    }

    // 带TTL（生存时间）的去重处理器
    public static class TTLVehicleDeduplicationProcessor extends RichFlatMapFunction<String, String> {

        private transient Set<String> processedVehicles;
        private transient ConcurrentHashMap<String, Long> vehicleTimestamps;
        private final long ttlMs; // 生存时间（毫秒）

        public TTLVehicleDeduplicationProcessor(long ttlMs) {
            this.ttlMs = ttlMs;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            processedVehicles = ConcurrentHashMap.newKeySet();
            vehicleTimestamps = new ConcurrentHashMap<>();
        }

        @Override
        public void flatMap(String jsonString, Collector<String> out) throws Exception {
            // 清理过期的车辆记录
            cleanupExpiredVehicles();

            JSONObject jsonObject = new JSONObject(jsonString);
            JSONArray pathList = jsonObject.getJSONArray("pathList");
            long currentTime = System.currentTimeMillis();

            for (int i = 0; i < pathList.length(); i++) {
                JSONObject vehicleData = pathList.getJSONObject(i);
                String vehicleId = String.valueOf(vehicleData.getLong("id"));
                String plateNo = vehicleData.getString("plateNo");

                String dedupKey = vehicleId + "_" + plateNo;

                if (!processedVehicles.contains(dedupKey)) {
                    // 新车处理
                    JSONObject output = createVehicleOutput(jsonObject, vehicleData, vehicleId, plateNo);
                    out.collect(output.toString());

                    processedVehicles.add(dedupKey);
                    vehicleTimestamps.put(dedupKey, currentTime);

                    System.out.println("New vehicle detected and cached: " + plateNo + " (ID: " + vehicleId + ")");
                } else {
                    // 更新车辆时间戳
                    vehicleTimestamps.put(dedupKey, currentTime);
                }
            }
        }

        private void cleanupExpiredVehicles() {
            long currentTime = System.currentTimeMillis();
            Set<String> expiredVehicles = new HashSet<>();

            for (String vehicleKey : vehicleTimestamps.keySet()) {
                Long timestamp = vehicleTimestamps.get(vehicleKey);
                if (timestamp != null && (currentTime - timestamp) > ttlMs) {
                    expiredVehicles.add(vehicleKey);
                }
            }

            // 移除过期车辆
            for (String expiredKey : expiredVehicles) {
                processedVehicles.remove(expiredKey);
                vehicleTimestamps.remove(expiredKey);
                System.out.println("Removed expired vehicle: " + expiredKey);
            }
        }

        private JSONObject createVehicleOutput(JSONObject sourceData, JSONObject vehicleData,
                                               String vehicleId, String plateNo) {
            JSONObject output = new JSONObject();
            output.put("eventType", "NEW_VEHICLE");
            output.put("vehicleId", vehicleId);
            output.put("plateNo", plateNo);
            output.put("firstSeenTime", sourceData.getString("timeStamp"));
            output.put("longitude", vehicleData.getDouble("longitude"));
            output.put("latitude", vehicleData.getDouble("latitude"));
            output.put("laneNo", vehicleData.getInt("laneNo"));
            output.put("vehicleType", vehicleData.getInt("originalType"));
            output.put("direction", vehicleData.optInt("direction", -1));
            output.put("speed", vehicleData.getDouble("speed"));

            // 可选字段
            if (vehicleData.has("vehicleColor")) {
                output.put("vehicleColor", vehicleData.getInt("vehicleColor"));
            }
            if (vehicleData.has("vehicleWeight")) {
                output.put("vehicleWeight", vehicleData.getDouble("vehicleWeight"));
            }
            if (vehicleData.has("specialFlag")) {
                output.put("specialFlag", vehicleData.getString("specialFlag"));
            }

            return output;
        }
    }

    // 使用示例 - 在您的Flink作业中使用：
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka配置
        String brokers = "10.48.53.82:9092";
        String groupId = "vehicle-deduplication-group";

        // 数据源
        List<String> topics = Arrays.asList("jtkj.jga.path.1");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> sourceStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 使用去重处理器 - 选择其中一种
        DataStream<String> deduplicatedStream = sourceStream
                .keyBy(jsonString -> {
                    // 按车辆ID分组，确保相同车辆ID的数据发送到同一个任务槽
                    try {
                        JSONObject jsonObject = new JSONObject(jsonString);
                        JSONArray pathList = jsonObject.getJSONArray("pathList");
                        if (pathList.length() > 0) {
                            JSONObject firstVehicle = pathList.getJSONObject(0);
                            return String.valueOf(firstVehicle.getLong("id"));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return "unknown";
                })
                .flatMap(new TTLVehicleDeduplicationProcessor(30 * 60 * 1000)) // 30分钟TTL
                .name("Vehicle Deduplication Processor");

        // 输出到Kafka
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("deduplicated_vehicles")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        deduplicatedStream.sinkTo(sink).name("Deduplicated Output Sink");

        env.execute("Vehicle Deduplication Job");
    }
}