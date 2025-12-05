package whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.jtmn;

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

public class ASJob {

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
        private double carAngle;
        private long timestamp; // 新增时间戳字段
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(11);

        // ================== Kafka 配置 ==================
        String primaryBrokers = "10.48.53.82:9092";
        String secondaryBrokers = "10.48.53.82:9092";
        String groupId = "combined-group";

        // ================== 主数据源 (fiberData1-11) ==================
        List<String> primaryTopics = Arrays.asList("jtkj.jga.path");

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

        // ================== 创建Kafka输出Sink ==================
        KafkaSink<String> primarySink = KafkaSink.<String>builder()
                .setBootstrapServers(primaryBrokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("trajectoryoutput")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        KafkaSink<String> secondarySink = KafkaSink.<String>builder()
                .setBootstrapServers(secondaryBrokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("zaOutPut")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

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

        // ================== 输出到Kafka ==================
        primaryKafkaOutput.sinkTo(primarySink).name("Primary Kafka Sink");
        secondaryKafkaOutput.sinkTo(secondarySink).name("Secondary Kafka Sink");

        // ================== 输出到HBase ==================
        primaryHBaseOutput.addSink(new PrimaryHBaseSink("ZCarTraj", "cf0"))
                .name("Primary HBase Sink")
                .setParallelism(2);

        secondaryHBaseOutput.addSink(new SecondaryHBaseSink("ZZaCarTraj", "cf0"))
                .name("Secondary HBase Sink")
                .setParallelism(2);

        env.execute("Combined Trajectory Analysis and Storage Job");
    }

    // ================== 主数据处理逻辑 (fiberData) ==================
    private static class PrimaryTrajectoryProcessor implements FlatMapFunction<String, String> {
        private static final long SESSION_TIMEOUT_MS = 100000;
        private static final long SAMPLING_INTERVAL_MS = 1000;

        private final Map<String, List<Tuple7<Double, Double, Integer, Integer, Double, Double, Long>>> map = new ConcurrentHashMap<>();
        private final Map<String, String> mapTimeSeg = new ConcurrentHashMap<>();
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
                            initializeNewVehicle(id, plateNo, tdataObject, timeObs);
                        } else {
                            updateVehicleTrajectory(id, tdataObject, timeObs);
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

            List<Tuple7<Double, Double, Integer, Integer, Double, Double, Long>> list = new ArrayList<>();
            list.add(new Tuple7<>(
                    tdata.optDouble("longitude", 0.0),
                    tdata.optDouble("latitude", 0.0),
                    tdata.optInt("laneNo", -1),
                    getDirectionSafely(tdata),
                    tdata.optDouble("speed", 0.0),
                    tdata.optDouble("carAngle", 0.0),
                    timestamp // 存储时间戳
            ));
            map.put(id, list);
        }

        private void updateVehicleTrajectory(String id, JSONObject tdata, long timestamp) {
            List<Tuple7<Double, Double, Integer, Integer, Double, Double, Long>> list = map.get(id);
            list.add(new Tuple7<>(
                    tdata.optDouble("longitude", 0.0),
                    tdata.optDouble("latitude", 0.0),
                    tdata.optInt("laneNo", -1),
                    getDirectionSafely(tdata),
                    tdata.optDouble("speed", 0.0),
                    tdata.optDouble("carAngle", 0.0),
                    timestamp // 存储时间戳
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
                if (lastSeenTime.getOrDefault(id, 0L) > currentTime - SESSION_TIMEOUT_MS) {
                    continue;
                }

                JSONObject trajectoryJson = new JSONObject();
                trajectoryJson.put("timeSeg", mapTimeSeg.get(id));
                trajectoryJson.put("type", mapType.get(id));
                trajectoryJson.put("latestTime", lastSeenTime.get(id));
                trajectoryJson.put("eventList", new JSONArray());

                JSONArray trajectoryArray = new JSONArray();
                for (Tuple7<Double, Double, Integer, Integer, Double, Double, Long> point : map.get(id)) {
                    JSONObject pointJson = new JSONObject();
                    pointJson.put("longitude", point.f0);
                    pointJson.put("latitude", point.f1);
                    pointJson.put("laneNo", point.f2);
                    pointJson.put("direction", point.f3);
                    pointJson.put("speed", point.f4);
                    pointJson.put("carAngle", point.f5);
                    pointJson.put("timestamp", point.f6); // 存储时间戳
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

    // ================== 辅助数据处理逻辑 (MergedPathData) ==================
    private static class SecondaryTrajectoryProcessor implements FlatMapFunction<String, String> {
        private static final long SESSION_TIMEOUT_MS = 10000;
        private static final long SAMPLING_INTERVAL_MS = 1000;

        private final Map<String, List<Tuple7<Double, Double, Integer, Integer, Double, Double, Long>>> map = new ConcurrentHashMap<>();
        private final Map<String, String> mapTimeSeg = new ConcurrentHashMap<>();
        private final Map<String, String> mapPlateNo = new ConcurrentHashMap<>();
        private final Map<String, Long> mapFirstSeenTime = new ConcurrentHashMap<>();
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
                            initializeNewVehicle(id, plateNo, tdataObject, timeObs);
                        } else {
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
                        return Instant.parse(timestampStr).toEpochMilli();
                    } catch (Exception e3) {
                        System.err.println("无法解析时间戳: " + timestampStr);
                        return 0;
                    }
                }
            }
        }

        private void initializeNewVehicle(String id, String plateNo, JSONObject tdata, long timestamp) {
            mapFirstSeenTime.put(id, timestamp);
            mapPlateNo.put(id, plateNo);
            updateTimeSeg(id);

            mapType.put(id, tdata.optInt("originalType", -1));

            List<Tuple7<Double, Double, Integer, Integer, Double, Double, Long>> list = new ArrayList<>();
            list.add(new Tuple7<>(
                    tdata.optDouble("longitude", 0.0),
                    tdata.optDouble("latitude", 0.0),
                    tdata.optInt("laneNo", -1),
                    getDirectionSafely(tdata),
                    tdata.optDouble("speed", 0.0),
                    tdata.optDouble("carAngle", 0.0),
                    timestamp // 存储时间戳
            ));
            map.put(id, list);
        }

        private void updateVehicleInfo(String id, String plateNo, JSONObject tdata, long timestamp) {
            List<Tuple7<Double, Double, Integer, Integer, Double, Double, Long>> list = map.get(id);
            list.add(new Tuple7<>(
                    tdata.optDouble("longitude", 0.0),
                    tdata.optDouble("latitude", 0.0),
                    tdata.optInt("laneNo", 0),
                    getDirectionSafely(tdata),
                    tdata.optDouble("speed", 0.0),
                    tdata.optDouble("carAngle", 0.0),
                    timestamp // 存储时间戳
            ));

            String currentPlateNo = mapPlateNo.get(id);
            if ((currentPlateNo.isEmpty() && !plateNo.isEmpty()) ||
                    (!plateNo.isEmpty() && !plateNo.equals(currentPlateNo))) {
                System.out.println("更新车辆 " + id + " 的车牌: " + currentPlateNo + " -> " + plateNo);
                mapPlateNo.put(id, plateNo);
                updateTimeSeg(id);
            }
        }

        private void updateTimeSeg(String id) {
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
                if (lastSeenTime.getOrDefault(id, 0L) > currentTime - SESSION_TIMEOUT_MS) {
                    continue;
                }

                JSONObject trajectoryJson = new JSONObject();
                trajectoryJson.put("timeSeg", mapTimeSeg.get(id));
                trajectoryJson.put("type", mapType.get(id));
                trajectoryJson.put("latestTime", lastSeenTime.get(id));

                JSONArray trajectoryArray = new JSONArray();
                for (Tuple7<Double, Double, Integer, Integer, Double, Double, Long> point : map.get(id)) {
                    JSONObject pointJson = new JSONObject();
                    pointJson.put("longitude", point.f0);
                    pointJson.put("latitude", point.f1);
                    pointJson.put("laneNo", point.f2);
                    pointJson.put("direction", point.f3);
                    pointJson.put("speed", point.f4);
                    pointJson.put("carAngle", point.f5);
                    pointJson.put("timestamp", point.f6); // 存储时间戳
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

                if (trajectoryArray == null || trajectoryArray.length() == 0) {
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
                            point.optDouble("speed", 0.0),
                            point.optDouble("carAngle", 0.0),
                            point.optLong("timestamp", 0L) // 解析时间戳
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

                if (trajectoryArray == null || trajectoryArray.length() == 0) {
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
                            point.optDouble("speed", 0.0),
                            point.optDouble("carAngle", 0.0),
                            point.optLong("timestamp", 0L) // 解析时间戳
                    ));
                }
                out.collect(new Tuple5<>(timeSeg, type, latestTime, trajectory, dir));
            } catch (JSONException e) {
                System.err.println("辅助数据解析失败: " + e.getMessage());
                System.err.println("原始数据: " + jsonString);
            }
        }
    }

    // ================== 主数据HBase Sink ==================
    private static class PrimaryHBaseSink extends RichSinkFunction<Tuple6<String, Integer, Long,
            List<TrajectoryPoint>, Integer, String>> {

        private final String baseTableName;
        private final String columnFamily;
        private transient Connection connection;
        private transient Table currentTable;
        private transient String currentTableName;
        private transient ReentrantLock tableLock;
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

        public PrimaryHBaseSink(String baseTableName, String columnFamily) {
            this.baseTableName = baseTableName;
            this.columnFamily = columnFamily;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            Configuration conf = createHBaseConfig();
            connection = ConnectionFactory.createConnection(conf);
            tableLock = new ReentrantLock();
        }

        @Override
        public void invoke(Tuple6<String, Integer, Long, List<TrajectoryPoint>,
                Integer, String> value, Context context) throws Exception {

            tableLock.lock();
            try {
                if (value.f3.size() <= 2) {
                    return;
                }
                String rowKey = value.f0;
                long rowKeyTime = parseRowKeyTime(rowKey);

                switchTableIfNeeded(rowKeyTime);

                // 使用Tuple6存储轨迹点，包含时间戳
                List<Tuple6<Double, Double, Integer, Double, Double, Long>> trajectoryList = new ArrayList<>();
                for (TrajectoryPoint point : value.f3) {
                    trajectoryList.add(new Tuple6<>(
                            point.getLongitude(),
                            point.getLatitude(),
                            point.getLaneNo(),
                            point.getSpeed(),
                            point.getCarAngle(),
                            point.getTimestamp() // 存储时间戳
                    ));
                }

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), Bytes.toBytes(value.f1.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("event_list"), Bytes.toBytes(value.f5));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("latest_time"), Bytes.toBytes(value.f2.toString()));
                // 存储包含时间戳的轨迹数据
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("trajectory"), Bytes.toBytes(trajectoryList.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("direction"), Bytes.toBytes(value.f4.toString()));

                currentTable.put(put);
                System.out.println("主数据写入HBase: " + rowKey + ", 轨迹点数: " + trajectoryList.size());
            } catch (Exception e) {
                System.err.println("主数据HBase写入失败: " + e.getMessage());
            } finally {
                tableLock.unlock();
            }
        }

        private long parseRowKeyTime(String rowKey) {
            try {
                return Long.parseLong(rowKey.split("-")[0]);
            } catch (NumberFormatException e) {
                System.err.println("无效的主数据rowKey格式: " + rowKey);
                return System.currentTimeMillis();
            }
        }

        private void switchTableIfNeeded(long rowKeyTime) throws IOException {
            LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault()
            );
            String newTableName = baseTableName + "_" + rowKeyDateTime.format(DateTimeFormatter.BASIC_ISO_DATE);

            if (currentTable == null || !newTableName.equals(currentTableName)) {
                tableLock.lock();
                try {
                    if (currentTable == null || !newTableName.equals(currentTableName)) {
                        createTableIfNotExists(newTableName);
                        if (currentTable != null) currentTable.close();
                        currentTable = connection.getTable(TableName.valueOf(newTableName));
                        currentTableName = newTableName;
                        System.out.println("主数据切换到HBase表: " + currentTableName);
                    }
                } finally {
                    tableLock.unlock();
                }
            }
        }

        private void createTableIfNotExists(String tableName) throws IOException {
            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());
            synchronized (lock) {
                try (Admin admin = connection.getAdmin()) {
                    TableName tn = TableName.valueOf(tableName);
                    if (!admin.tableExists(tn)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(tn);
                        HColumnDescriptor cfDesc = new HColumnDescriptor(columnFamily);
                        tableDescriptor.addFamily(cfDesc);
                        try {
                            admin.createTable(tableDescriptor);
                            System.out.println("创建主数据HBase表: " + tableName);
                        } catch (TableExistsException e) {
                            System.out.println("主数据HBase表已存在: " + tableName);
                        }
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            try {
                if (currentTable != null) currentTable.close();
            } finally {
                if (connection != null) connection.close();
            }
        }
    }

    // ================== 辅助数据HBase Sink ==================
    private static class SecondaryHBaseSink extends RichSinkFunction<Tuple5<String, Integer, Long,
            List<TrajectoryPoint>, Integer>> {

        private final String baseTableName;
        private final String columnFamily;
        private transient Connection connection;
        private transient Table currentTable;
        private transient String currentTableName;
        private transient ReentrantLock tableLock;
        private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();

        public SecondaryHBaseSink(String baseTableName, String columnFamily) {
            this.baseTableName = baseTableName;
            this.columnFamily = columnFamily;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            Configuration conf = createHBaseConfig();
            connection = ConnectionFactory.createConnection(conf);
            tableLock = new ReentrantLock();
        }

        @Override
        public void invoke(Tuple5<String, Integer, Long, List<TrajectoryPoint>,
                Integer> value, Context context) throws Exception {

            tableLock.lock();
            try {
                if (value.f3.size() <= 2) {
                    System.out.println("辅助数据Sink端轨迹点不足2个，跳过: " + value.f0);
                    return;
                }

                // 使用Tuple5存储轨迹点，包含时间戳
                List<Tuple5<Double, Double, Integer, Double, Long>> trajectoryList = new ArrayList<>();
                for (TrajectoryPoint point : value.f3) {
                    trajectoryList.add(new Tuple5<>(
                            point.getLongitude(),
                            point.getLatitude(),
                            point.getLaneNo(),
                            point.getSpeed(),
                            point.getTimestamp() // 存储时间戳
                    ));
                }

                String rowKey = value.f0;
                long rowKeyTime = parseRowKeyTime(rowKey);

                switchTableIfNeeded(rowKeyTime);

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("type"), Bytes.toBytes(value.f1.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("latest_time"), Bytes.toBytes(value.f2.toString()));
                // 存储包含时间戳的轨迹数据
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("trajectory"), Bytes.toBytes(trajectoryList.toString()));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("direction"), Bytes.toBytes(value.f4.toString()));

                currentTable.put(put);
                System.out.println("辅助数据写入HBase: " + rowKey + ", 轨迹点数: " + trajectoryList.size());
            } catch (Exception e) {
                System.err.println("辅助数据HBase写入失败: " + e.getMessage());
            } finally {
                tableLock.unlock();
            }
        }

        private long parseRowKeyTime(String rowKey) {
            try {
                return Long.parseLong(rowKey.split("-")[0]);
            } catch (NumberFormatException e) {
                System.err.println("无效的辅助数据rowKey格式: " + rowKey);
                return System.currentTimeMillis();
            }
        }

        private void switchTableIfNeeded(long rowKeyTime) throws IOException {
            LocalDateTime rowKeyDateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(rowKeyTime), ZoneId.systemDefault()
            );
            String newTableName = baseTableName + "_" + rowKeyDateTime.format(DateTimeFormatter.BASIC_ISO_DATE);

            if (currentTable == null || !newTableName.equals(currentTableName)) {
                tableLock.lock();
                try {
                    if (currentTable == null || !newTableName.equals(currentTableName)) {
                        createTableIfNotExists(newTableName);
                        if (currentTable != null) currentTable.close();
                        currentTable = connection.getTable(TableName.valueOf(newTableName));
                        currentTableName = newTableName;
                        System.out.println("辅助数据切换到HBase表: " + currentTableName);
                    }
                } finally {
                    tableLock.unlock();
                }
            }
        }

        private void createTableIfNotExists(String tableName) throws IOException {
            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());
            synchronized (lock) {
                try (Admin admin = connection.getAdmin()) {
                    TableName tn = TableName.valueOf(tableName);
                    if (!admin.tableExists(tn)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(tn);
                        HColumnDescriptor cfDesc = new HColumnDescriptor(columnFamily);
                        tableDescriptor.addFamily(cfDesc);
                        try {
                            admin.createTable(tableDescriptor);
                            System.out.println("创建辅助数据HBase表: " + tableName);
                        } catch (TableExistsException e) {
                            System.out.println("辅助数据HBase表已存在: " + tableName);
                        }
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            try {
                if (currentTable != null) currentTable.close();
            } finally {
                if (connection != null) connection.close();
            }
        }
    }

    // ================== 公共配置方法 ==================
    private static Configuration createHBaseConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.session.timeout", "120000");
        conf.set("hbase.rpc.timeout", "300000");
        conf.set("fs.defaultFS", "hdfs://100.65.38.139:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        return conf;
    }

    // ================== 自定义Tuple7类 ==================
    public static class Tuple7<T0, T1, T2, T3, T4, T5, T6> {
        public T0 f0;
        public T1 f1;
        public T2 f2;
        public T3 f3;
        public T4 f4;
        public T5 f5;
        public T6 f6;

        public Tuple7() {}

        public Tuple7(T0 f0, T1 f1, T2 f2, T3 f3, T4 f4, T5 f5, T6 f6) {
            this.f0 = f0;
            this.f1 = f1;
            this.f2 = f2;
            this.f3 = f3;
            this.f4 = f4;
            this.f5 = f5;
            this.f6 = f6;
        }

        @Override
        public String toString() {
            return "(" + f0 + "," + f1 + "," + f2 + "," + f3 + "," + f4 + "," + f5 + "," + f6 + ")";
        }
    }
}