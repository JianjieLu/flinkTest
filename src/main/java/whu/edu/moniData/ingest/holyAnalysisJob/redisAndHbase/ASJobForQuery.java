package whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class ASJobForQuery {

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class TrajectoryPoint {
        private double longitude;
        private double latitude;
        private int laneNo;
        private int direction;
        private double speed;
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(11);

        // ================== Kafka 配置 ==================
        String primaryBrokers = "10.48.53.82:9092";
        String secondaryBrokers = "10.48.53.82:9092";
        String groupId = "combined-group";

        // ================== 主数据源 (fiberData1-11) ==================
        List<String> primaryTopics = Arrays.asList(
                "fiberData1", "fiberData2", "fiberData3",
                "fiberData4", "fiberData5", "fiberData6",
                "fiberData7", "fiberData8", "fiberData9",
                "fiberData10", "fiberData11");

        KafkaSource<String> primarySource = KafkaSource.<String>builder()
                .setBootstrapServers(primaryBrokers)
                .setTopics(primaryTopics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setProperty("consumer.max.poll.interval.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setProperty("session.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> primaryStream = env.fromSource(
                primarySource, WatermarkStrategy.noWatermarks(), "Primary Kafka Source");

        // ================== 辅助数据源 (MergedPathData) ==================
        List<String> secondaryTopics = Collections.singletonList("MergedRampPathData");

        KafkaSource<String> secondarySource = KafkaSource.<String>builder()
                .setBootstrapServers(secondaryBrokers)
                .setTopics(secondaryTopics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setProperty("consumer.max.poll.interval.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setProperty("session.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> secondaryStream = env.fromSource(
                secondarySource, WatermarkStrategy.noWatermarks(), "Secondary Kafka Source");

        // ================== 处理主数据流 ==================
        SingleOutputStreamOperator<String> primaryKafkaOutput = primaryStream
                .flatMap(new PrimaryTrajectoryProcessor())
                .name("Primary Trajectory Processor");

        SingleOutputStreamOperator<Tuple6<String, Integer, Long, List<TrajectoryPoint>, Integer, String>> primaryHBaseOutput =
                primaryKafkaOutput.flatMap(new PrimaryJSONParser())
                        .name("Primary Data Parser");

        // ================== 处理辅助数据流 ==================
        SingleOutputStreamOperator<String> secondaryKafkaOutput = secondaryStream
                .flatMap(new SecondaryTrajectoryProcessor())
                .name("Secondary Trajectory Processor");

        SingleOutputStreamOperator<Tuple5<String, Integer, Long, List<TrajectoryPoint>, Integer>> secondaryHBaseOutput =
                secondaryKafkaOutput.flatMap(new SecondaryJSONParser())
                        .name("Secondary Data Parser");

        env.execute("Combined Trajectory Analysis and Storage Job");
    }

    // ================== 主数据处理逻辑 (fiberData) ==================
    private static class PrimaryTrajectoryProcessor implements FlatMapFunction<String, String> {
        private static final long SESSION_TIMEOUT_MS = 100000;
        private static final long SAMPLING_INTERVAL_MS = 1000;
        // 状态存储
        private final Map<String, List<Tuple5<Double, Double, Integer, Integer, Double>>> map = new ConcurrentHashMap<>();
        private final Map<String, String> mapTimeSeg = new ConcurrentHashMap<>();
        private final Map<String, Integer> mapType = new ConcurrentHashMap<>();
        private final Map<String, Long> lastSeenTime = new ConcurrentHashMap<>();
        private final Map<String, Long> lastSampleTime = new ConcurrentHashMap<>();
        private final ReentrantLock stateLock = new ReentrantLock();
        // 添加查询方法
        public List<TrajectoryPoint> queryTrajectoryByPlateNo(String plateNo) {
            List<TrajectoryPoint> result = new ArrayList<>();
            for (Map.Entry<String, String> entry : mapTimeSeg.entrySet()) {
                String timeSeg = entry.getValue();
                // timeSeg格式: timestamp-plateNo-id
                String[] parts = timeSeg.split("-");
                if (parts.length >= 2 && parts[1].equals(plateNo)) {
                    String id = entry.getKey();
                    List<Tuple5<Double, Double, Integer, Integer, Double>> points = map.get(id);
                    if (points != null) {
                        for (Tuple5<Double, Double, Integer, Integer, Double> point : points) {
                            result.add(new TrajectoryPoint(
                                    point.f0, point.f1, point.f2, point.f3, point.f4
                            ));
                        }
                    }
                }
            }
            return result;
        }

        // 根据ID查询
        public List<TrajectoryPoint> queryTrajectoryById(String id) {
            List<Tuple5<Double, Double, Integer, Integer, Double>> points = map.get(id);
            if (points == null) return new ArrayList<>();

            List<TrajectoryPoint> result = new ArrayList<>();
            for (Tuple5<Double, Double, Integer, Integer, Double> point : points) {
                result.add(new TrajectoryPoint(point.f0, point.f1, point.f2, point.f3, point.f4));
            }
            return result;
        }

        // 获取所有活跃车辆
        public Map<String, String> getAllActiveVehicles() {
            return new HashMap<>(mapTimeSeg);
        }
        @Override
        public void flatMap(String jsonString, Collector<String> out) {
            stateLock.lock();
            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                long timeObs = parseTimestamp(jsonObject.optString("timeStamp", ""));
                if (timeObs == 0) {
                    System.err.println("无法解析时间戳: " + jsonObject.optString("timeStamp", ""));
                    return;
                }

                JSONArray tdataArray = jsonObject.optJSONArray("pathList");
                if (tdataArray == null) {
                    System.err.println("缺少pathList字段: " + jsonString);
                    return;
                }

                for (int i = 0; i < tdataArray.length(); i++) {
                    JSONObject tdataObject = tdataArray.optJSONObject(i);
                    if (tdataObject == null) continue;

                    String plateNo = tdataObject.optString("plateNo", "");
                    String id = String.valueOf(tdataObject.optLong("id", -1));
                    if (id.equals("-1")) continue;

                    lastSeenTime.put(id, timeObs);
                    long lastSample = lastSampleTime.getOrDefault(id, 0L);

                    if (timeObs - lastSample >= SAMPLING_INTERVAL_MS) {
                        if (!map.containsKey(id)) {
                            initializeNewVehicle(id, plateNo, tdataObject, timeObs);
                        } else {
                            updateVehicleTrajectory(id, tdataObject);
                        }
                        lastSampleTime.put(id, timeObs);
                    }
                }

                processTimeoutVehicles(timeObs, out);
            } catch (Exception e) {
                System.err.println("处理主数据时发生异常: " + e.getMessage());
                e.printStackTrace();
            } finally {
                stateLock.unlock();
            }
        }

        private long parseTimestamp(String timestampStr) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception e1) {
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                    LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                    return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                } catch (Exception e2) {
                    try {
                        // 尝试ISO格式
                        return Instant.parse(timestampStr).toEpochMilli();
                    } catch (Exception e3) {
                        System.err.println("无法解析时间戳: " + timestampStr);
                        return 0;
                    }
                }
            }
        }

        private void initializeNewVehicle(String id, String plateNo, JSONObject tdata, long timestamp) {
            mapTimeSeg.put(id, timestamp + "-" + plateNo + "-" + id);
            mapType.put(id, tdata.optInt("vehicleType", -1));

            List<Tuple5<Double, Double, Integer, Integer, Double>> list = new ArrayList<>();
            list.add(new Tuple5<>(
                    tdata.optDouble("longitude", 0.0),
                    tdata.optDouble("latitude", 0.0),
                    tdata.optInt("laneNo", -1),
                    getDirectionSafely(tdata),
                    tdata.optDouble("speed", 0.0)
            ));
            map.put(id, list);
        }

        private void updateVehicleTrajectory(String id, JSONObject tdata) {
            List<Tuple5<Double, Double, Integer, Integer, Double>> list = map.get(id);
            list.add(new Tuple5<>(
                    tdata.optDouble("longitude", 0.0),
                    tdata.optDouble("latitude", 0.0),
                    tdata.optInt("laneNo", -1),
                    getDirectionSafely(tdata),
                    tdata.optDouble("speed", 0.0)
            ));
        }

        private void processTimeoutVehicles(long currentTime, Collector<String> out) {
            Set<String> timeoutIds = new HashSet<>();
            for (Map.Entry<String, Long> entry : lastSeenTime.entrySet()) {
                if (currentTime - entry.getValue() > SESSION_TIMEOUT_MS) {
                    timeoutIds.add(entry.getKey());
                }
            }

            for (String id : timeoutIds) {
                // 二次检查，确保车辆确实超时
                if (lastSeenTime.getOrDefault(id, 0L) > currentTime - SESSION_TIMEOUT_MS) {
                    continue;
                }

                JSONObject trajectoryJson = new JSONObject();
                trajectoryJson.put("timeSeg", mapTimeSeg.get(id));
                trajectoryJson.put("type", mapType.get(id));
                trajectoryJson.put("latestTime", lastSeenTime.get(id));
                trajectoryJson.put("eventList", new JSONArray());

                JSONArray trajectoryArray = new JSONArray();
                for (Tuple5<Double, Double, Integer, Integer, Double> point : map.get(id)) {
                    JSONObject pointJson = new JSONObject();
                    pointJson.put("longitude", point.f0);
                    pointJson.put("latitude", point.f1);
                    pointJson.put("laneNo", point.f2);
                    pointJson.put("direction", point.f3);
                    pointJson.put("speed", point.f4);
                    trajectoryArray.put(pointJson);
                }
                trajectoryJson.put("trajectory", trajectoryArray);

                out.collect(trajectoryJson.toString());
                cleanupVehicle(id);
            }
        }

        private void cleanupVehicle(String id) {
            map.remove(id);
            mapTimeSeg.remove(id);
            mapType.remove(id);
            lastSeenTime.remove(id);
            lastSampleTime.remove(id);
        }

        private int getDirectionSafely(JSONObject tdata) {
            return tdata.optInt("direction", -1);
        }
    }

    // ================== 辅助数据处理逻辑 (MergedPathData) - 修改版本 ==================
    private static class SecondaryTrajectoryProcessor implements FlatMapFunction<String, String> {
        private static final long SESSION_TIMEOUT_MS = 10000;
        private static final long SAMPLING_INTERVAL_MS = 1000;

        // 独立状态存储
        private final Map<String, List<Tuple5<Double, Double, Integer, Integer, Double>>> map = new ConcurrentHashMap<>();
        private final Map<String, String> mapTimeSeg = new ConcurrentHashMap<>(); // 存储格式: timestamp-plateNo-id
        private final Map<String, String> mapPlateNo = new ConcurrentHashMap<>(); // 单独存储车牌号，便于更新
        private final Map<String, Long> mapFirstSeenTime = new ConcurrentHashMap<>(); // 存储首次出现时间
        private final Map<String, Integer> mapType = new ConcurrentHashMap<>();
        private final Map<String, Long> lastSeenTime = new ConcurrentHashMap<>();
        private final Map<String, Long> lastSampleTime = new ConcurrentHashMap<>();
        private final ReentrantLock stateLock = new ReentrantLock();

        @Override
        public void flatMap(String jsonString, Collector<String> out) {
            stateLock.lock();
            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                long timeObs = parseTimestamp(jsonObject.optString("timeStamp", ""));
                if (timeObs == 0) {
                    System.err.println("无法解析时间戳: " + jsonObject.optString("timeStamp", ""));
                    return;
                }

                JSONArray tdataArray = jsonObject.optJSONArray("pathList");
                if (tdataArray == null) {
                    System.err.println("缺少pathList字段: " + jsonString);
                    return;
                }

                for (int i = 0; i < tdataArray.length(); i++) {
                    JSONObject tdataObject = tdataArray.optJSONObject(i);
                    if (tdataObject == null) continue;

                    String plateNo = tdataObject.optString("plateNo", "");
                    String id = String.valueOf(tdataObject.optLong("id", -1));
                    if (id.equals("-1")) continue;

                    lastSeenTime.put(id, timeObs);
                    long lastSample = lastSampleTime.getOrDefault(id, 0L);

                    if (timeObs - lastSample >= SAMPLING_INTERVAL_MS) {
                        if (!map.containsKey(id)) {
                            // 第一次看到该车辆
                            initializeNewVehicle(id, plateNo, tdataObject, timeObs);
                        } else {
                            // 更新现有车辆信息
                            updateVehicleInfo(id, plateNo, tdataObject, timeObs);
                        }
                        lastSampleTime.put(id, timeObs);
                    }
                }

                processTimeoutVehicles(timeObs, out);
            } catch (Exception e) {
                System.err.println("处理辅助数据时发生异常: " + e.getMessage());
                e.printStackTrace();
            } finally {
                stateLock.unlock();
            }
        }

        private long parseTimestamp(String timestampStr) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception e1) {
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                    LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                    return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                } catch (Exception e2) {
                    try {
                        // 尝试ISO格式
                        return Instant.parse(timestampStr).toEpochMilli();
                    } catch (Exception e3) {
                        System.err.println("无法解析时间戳: " + timestampStr);
                        return 0;
                    }
                }
            }
        }

        private void initializeNewVehicle(String id, String plateNo, JSONObject tdata, long timestamp) {
            // 存储首次出现时间
            mapFirstSeenTime.put(id, timestamp);
            // 存储车牌号（可能为空）
            mapPlateNo.put(id, plateNo);
            // 生成初始timeSeg
            updateTimeSeg(id);

            mapType.put(id, tdata.optInt("originalType", -1));

            List<Tuple5<Double, Double, Integer, Integer, Double>> list = new ArrayList<>();
            list.add(new Tuple5<>(
                    tdata.optDouble("longitude", 0.0),
                    tdata.optDouble("latitude", 0.0),
                    tdata.optInt("laneNo", -1),
                    getDirectionSafely(tdata),
                    tdata.optDouble("speed", 0.0)
            ));
            map.put(id, list);
        }

        private void updateVehicleInfo(String id, String plateNo, JSONObject tdata, long timestamp) {
            // 更新轨迹点
            List<Tuple5<Double, Double, Integer, Integer, Double>> list = map.get(id);
            list.add(new Tuple5<>(
                    tdata.optDouble("longitude", 0.0),
                    tdata.optDouble("latitude", 0.0),
                    tdata.optInt("laneNo", 0),
                    getDirectionSafely(tdata),
                    tdata.optDouble("speed", 0.0)
            ));

            // 如果之前没有车牌，现在有车牌了，或者车牌发生了变化，则更新车牌信息
            String currentPlateNo = mapPlateNo.get(id);
            if ((currentPlateNo.isEmpty() && !plateNo.isEmpty()) ||
                    (!plateNo.isEmpty() && !plateNo.equals(currentPlateNo))) {
                System.out.println("更新车辆 " + id + " 的车牌: " + currentPlateNo + " -> " + plateNo);
                mapPlateNo.put(id, plateNo);
                updateTimeSeg(id);
            }
        }

        private void updateTimeSeg(String id) {
            // 根据首次出现时间和当前车牌号更新timeSeg
            Long firstSeenTime = mapFirstSeenTime.get(id);
            String plateNo = mapPlateNo.get(id);
            if (firstSeenTime != null) {
                String newTimeSeg = firstSeenTime + "-" + plateNo + "-" + id;
                mapTimeSeg.put(id, newTimeSeg);
                System.out.println("更新车辆 " + id + " 的timeSeg: " + newTimeSeg);
            }
        }

        private void processTimeoutVehicles(long currentTime, Collector<String> out) {
            Set<String> timeoutIds = new HashSet<>();
            for (Map.Entry<String, Long> entry : lastSeenTime.entrySet()) {
                if (currentTime - entry.getValue() > SESSION_TIMEOUT_MS) {
                    timeoutIds.add(entry.getKey());
                }
            }

            for (String id : timeoutIds) {
                // 二次检查，确保车辆确实超时
                if (lastSeenTime.getOrDefault(id, 0L) > currentTime - SESSION_TIMEOUT_MS) {
                    continue;
                }

                JSONObject trajectoryJson = new JSONObject();
                trajectoryJson.put("timeSeg", mapTimeSeg.get(id));
                trajectoryJson.put("type", mapType.get(id));
                trajectoryJson.put("latestTime", lastSeenTime.get(id));

                JSONArray trajectoryArray = new JSONArray();
                for (Tuple5<Double, Double, Integer, Integer, Double> point : map.get(id)) {
                    JSONObject pointJson = new JSONObject();
                    pointJson.put("longitude", point.f0);
                    pointJson.put("latitude", point.f1);
                    pointJson.put("laneNo", point.f2);
                    pointJson.put("direction", point.f3);
                    pointJson.put("speed", point.f4);
                    trajectoryArray.put(pointJson);
                }
                trajectoryJson.put("trajectory", trajectoryArray);

                out.collect(trajectoryJson.toString());
                cleanupVehicle(id);
            }
        }

        private void cleanupVehicle(String id) {
            map.remove(id);
            mapTimeSeg.remove(id);
            mapPlateNo.remove(id);
            mapFirstSeenTime.remove(id);
            mapType.remove(id);
            lastSeenTime.remove(id);
            lastSampleTime.remove(id);
        }

        private int getDirectionSafely(JSONObject tdata) {
            return tdata.optInt("direction", -1);
        }
    }

    // ================== 主数据解析器 ==================
    private static class PrimaryJSONParser implements FlatMapFunction<String,
            Tuple6<String, Integer, Long, List<TrajectoryPoint>, Integer, String>> {

        @Override
        public void flatMap(String jsonString,
                            Collector<Tuple6<String, Integer, Long, List<TrajectoryPoint>, Integer, String>> out) {

            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                String timeSeg = jsonObject.optString("timeSeg", "");
                int type = jsonObject.optInt("type", -1);
                long latestTime = jsonObject.optLong("latestTime", 0L);
                JSONArray trajectoryArray = jsonObject.optJSONArray("trajectory");
                String eventList = jsonObject.optJSONArray("eventList").toString();

                if (trajectoryArray == null || trajectoryArray.isEmpty()) {
                    System.err.println("主数据缺少轨迹点: " + jsonString);
                    return;
                }

                int dir = trajectoryArray.getJSONObject(0).optInt("direction", -1);
                List<TrajectoryPoint> trajectory = new ArrayList<>();

                for (int i = 0; i < trajectoryArray.length(); i++) {
                    JSONObject point = trajectoryArray.optJSONObject(i);
                    if (point == null) continue;

                    trajectory.add(new TrajectoryPoint(
                            point.optDouble("longitude", 0.0),
                            point.optDouble("latitude", 0.0),
                            point.optInt("laneNo", -1),
                            point.optInt("direction", -1),
                            point.optDouble("speed", 0.0)
                    ));
                }
                out.collect(new Tuple6<>(timeSeg, type, latestTime, trajectory, dir, eventList));
            } catch (JSONException e) {
                System.err.println("主数据解析失败: " + e.getMessage());
                System.err.println("原始数据: " + jsonString);
            }
        }
    }

    // ================== 辅助数据解析器 ==================
    private static class SecondaryJSONParser implements FlatMapFunction<String,
            Tuple5<String, Integer, Long, List<TrajectoryPoint>, Integer>> {

        @Override
        public void flatMap(String jsonString,
                            Collector<Tuple5<String, Integer, Long, List<TrajectoryPoint>, Integer>> out) {

            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                String timeSeg = jsonObject.optString("timeSeg", "");
                int type = jsonObject.optInt("type", -1);
                long latestTime = jsonObject.optLong("latestTime", 0L);
                JSONArray trajectoryArray = jsonObject.optJSONArray("trajectory");

                if (trajectoryArray == null || trajectoryArray.isEmpty()) {
                    System.err.println("辅助数据缺少轨迹点: " + jsonString);
                    return;
                }

                int dir = trajectoryArray.getJSONObject(0).optInt("direction", -1);

                List<TrajectoryPoint> trajectory = new ArrayList<>();
                for (int i = 0; i < trajectoryArray.length(); i++) {
                    JSONObject point = trajectoryArray.optJSONObject(i);
                    if (point == null) continue;

                    trajectory.add(new TrajectoryPoint(
                            point.optDouble("longitude", 0.0),
                            point.optDouble("latitude", 0.0),
                            point.optInt("laneNo", -1),
                            point.optInt("direction", -1),
                            point.optDouble("speed", 0.0)
                    ));
                }
                out.collect(new Tuple5<>(timeSeg, type, latestTime, trajectory, dir));
            } catch (JSONException e) {
                System.err.println("辅助数据解析失败: " + e.getMessage());
                System.err.println("原始数据: " + jsonString);
            }
        }
    }
}