package whu.edu.moniData.ingest.holyAnalysisJob;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class CombinedCarTrajAnalysisJob_NoFeature_za {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ================== Kafka 配置 ==================
        String secondaryBrokers = "10.48.53.82:9092";
        String groupId = "flink-combined-group";

//        // ================== 主数据源 (fiberData1-11) ==================
//        List<String> primaryTopics = Arrays.asList(
//                "fiberData1", "fiberData2", "fiberData3",
//                "fiberData4", "fiberData5", "fiberData6",
//                "fiberData7", "fiberData8", "fiberData9",
//                "fiberData10", "fiberData11");




        // ================== 辅助数据源 (MergedPathData) ==================
        List<String> secondaryTopics = Collections.singletonList("MergedPathData");

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

        // ================== 创建输出Sink ==================


        // 辅助输出 (zaOutPut)
        KafkaSink<String> secondarySink = KafkaSink.<String>builder()
                .setBootstrapServers(secondaryBrokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("zaOutPut")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();


        // ================== 处理辅助数据流 ==================
        SingleOutputStreamOperator<String> secondaryProcessed = secondaryStream
                .flatMap(new SecondaryTrajectoryProcessor())
                .name("Secondary Trajectory Processor");

        // ================== 输出结果 ==================
        secondaryProcessed.sinkTo(secondarySink).name("Secondary Output Sink");

        env.execute("Trajectory Analysis Job (NoFeature)");
    }

    // ================== 主数据处理逻辑 (fiberData) ==================
    private static class PrimaryTrajectoryProcessor implements FlatMapFunction<String, String> {
        private static final long SESSION_TIMEOUT_MS = 10000;
        private static final long SAMPLING_INTERVAL_MS = 1000;

        // 状态存储 (隔离于其他处理器)
        private final Map<String, List<Tuple5<Double, Double, Integer, Integer, Double>>> map = new ConcurrentHashMap<>();
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
                long timeObs = parseTimestamp(jsonObject.getString("timeStamp"));
                JSONArray tdataArray = jsonObject.getJSONArray("pathList");

                for (int i = 0; i < tdataArray.length(); i++) {
                    JSONObject tdataObject = tdataArray.getJSONObject(i);
                    String plateNo = tdataObject.getString("plateNo");
                    String id = String.valueOf(tdataObject.getLong("id"));

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
                e.printStackTrace();
            } finally {
                stateLock.unlock();
            }
        }

        private long parseTimestamp(String timestampStr) throws Exception {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception e) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            }
        }

        private void initializeNewVehicle(String id, String plateNo, JSONObject tdata, long timestamp) {
            mapTimeSeg.put(id, timestamp + "-" + plateNo + "-" + id);
            mapType.put(id, tdata.getInt("vehicleType"));

            List<Tuple5<Double, Double, Integer, Integer, Double>> list = new ArrayList<>();
            list.add(new Tuple5<>(
                    tdata.getDouble("longitude"),
                    tdata.getDouble("latitude"),
                    tdata.getInt("laneNo"),
                    getDirectionSafely(tdata),
                    tdata.getDouble("speed")
            ));
            map.put(id, list);
        }

        private void updateVehicleTrajectory(String id, JSONObject tdata) {
            List<Tuple5<Double, Double, Integer, Integer, Double>> list = map.get(id);
            list.add(new Tuple5<>(
                    tdata.getDouble("longitude"),
                    tdata.getDouble("latitude"),
                    tdata.getInt("laneNo"),
                    getDirectionSafely(tdata),
                    tdata.getDouble("speed")
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

        // 安全获取方法
        private int getDirectionSafely(JSONObject tdata) {
            try { return tdata.getInt("direction"); }
            catch (JSONException e) { return -1; }
        }
    }

    // ================== 辅助数据处理逻辑 (MergedPathData) ==================
    private static class SecondaryTrajectoryProcessor implements FlatMapFunction<String, String> {
        private static final long SESSION_TIMEOUT_MS = 10000;
        private static final long SAMPLING_INTERVAL_MS = 1000;

        // 独立状态存储
        private final Map<String, List<Tuple5<Double, Double, Integer, Integer, Double>>> map = new ConcurrentHashMap<>();
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
                long timeObs = parseTimestamp(jsonObject.getString("timeStamp"));
                JSONArray tdataArray = jsonObject.getJSONArray("pathList");

                for (int i = 0; i < tdataArray.length(); i++) {
                    JSONObject tdataObject = tdataArray.getJSONObject(i);
//                    System.out.println("tdataObject:"+tdataObject);
                    String plateNo = tdataObject.getString("plateNo");
                    String id = String.valueOf(tdataObject.getLong("id"));

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
                e.printStackTrace();
            } finally {
                stateLock.unlock();
            }
        }

        private long parseTimestamp(String timestampStr) throws Exception {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception e) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            }
        }

        private void initializeNewVehicle(String id, String plateNo, JSONObject tdata, long timestamp) {
            mapTimeSeg.put(id, timestamp + "-" + plateNo + "-" + id);
            mapType.put(id, tdata.getInt("originalType"));

            List<Tuple5<Double, Double, Integer, Integer, Double>> list = new ArrayList<>();
            list.add(new Tuple5<>(
                    tdata.getDouble("longitude"),
                    tdata.getDouble("latitude"),
                    tdata.getInt("laneNo"),
                    getDirectionSafely(tdata),
                    tdata.getDouble("speed")
            ));
            map.put(id, list);
        }

        private void updateVehicleTrajectory(String id, JSONObject tdata) {
            List<Tuple5<Double, Double, Integer, Integer, Double>> list = map.get(id);
            list.add(new Tuple5<>(
                    tdata.getDouble("longitude"),
                    tdata.getDouble("latitude"),
                    tdata.getInt("laneNo"),
                    getDirectionSafely(tdata),
                    tdata.getDouble("speed")
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
            mapType.remove(id);
            lastSeenTime.remove(id);
            lastSampleTime.remove(id);
        }

        private int getDirectionSafely(JSONObject tdata) {
            try { return tdata.getInt("direction"); }
            catch (JSONException e) { return -1; }
        }
    }
}