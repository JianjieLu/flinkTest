package whu.edu.moniData.ingest.holyAnalysisJob;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class CombinedCarTrajAnalysisJob_NoFeature_Main {

    // 定义PathTData和PathPoint类
    @lombok.Data
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class PathTData {
        private int pathNum;
        private long time;
        private String timeStamp;
        private Integer segId;
        private List<PathPoint> pathList;
    }

    @lombok.Data
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class PathPoint {
        private int direction;
        private long id;
        private int laneNo;
        private double mileage;
        private String plateNo = "";
        private double speed;
        private String timeStamp;
        private Integer plateColor = null;
        private Integer vehicleType = null;
        private double longitude;
        private double latitude;
        private double carAngle;
        private String stakeId = "";
        private Integer originalType = null;
        private Integer originalColor = null;
        private String specialFlag = "";
        private String simulatedPlateNo = "";
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ================== Kafka 配置 ==================
        String primaryBrokers = "10.48.53.82:9092";
        String groupId = "flink-combined-group";

        List<String> primaryTopics = Arrays.asList(
                "fiberData1", "fiberData2", "fiberData3",
                "fiberData4", "fiberData5", "fiberData6",
                "fiberData7", "fiberData8", "fiberData9",
                "fiberData10", "fiberData11");

        // ================== 创建合并数据流 ==================
        DataStream<PathTData> unionStream = null;

        for (int i = 0; i < primaryTopics.size(); i++) {
            final int segmentId = i + 1;

            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(primaryBrokers)
                    .setTopics(primaryTopics.get(i))
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setProperty("auto.offset.commit", "true")
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            DataStream<PathTData> stream = env.fromSource(
                            source,
                            WatermarkStrategy.noWatermarks(),
                            "Kafka Source " + segmentId)
                    .map(jsonString -> {
                        JSONObject jsonObject = new JSONObject(jsonString);
                        PathTData pathData = new PathTData();
                        pathData.setPathNum(jsonObject.getJSONArray("pathList").length());
                        pathData.setTime(parseTimestamp(jsonObject.getString("timeStamp")));
                        pathData.setTimeStamp(jsonObject.getString("timeStamp"));

                        // 设置segId
                        if (segmentId == 5) {
                            pathData.setSegId(segmentId * 5);
                        } else if (segmentId == 7) {
                            pathData.setSegId(segmentId * 7 - 2);
                        } else if (segmentId == 8) {
                            pathData.setSegId(segmentId * 3);
                        } else {
                            pathData.setSegId(segmentId);
                        }

                        // 解析pathList
                        List<PathPoint> pathList = new ArrayList<>();
                        JSONArray pathArray = jsonObject.getJSONArray("pathList");

                        for (int j = 0; j < pathArray.length(); j++) {
                            JSONObject pointObj = pathArray.getJSONObject(j);
                            PathPoint point = new PathPoint();
                            point.setId(pointObj.getLong("id"));
                            point.setPlateNo(pointObj.getString("plateNo"));
                            point.setVehicleType(pointObj.getInt("vehicleType"));
                            point.setLongitude(pointObj.getDouble("longitude"));
                            point.setLatitude(pointObj.getDouble("latitude"));
                            point.setLaneNo(pointObj.getInt("laneNo"));
                            point.setDirection(getDirectionSafely(pointObj));
                            point.setSpeed(pointObj.getDouble("speed"));
                            point.setTimeStamp(jsonObject.getString("timeStamp"));

                            pathList.add(point);
                        }

                        pathData.setPathList(pathList);
                        return pathData;
                    });

            if (unionStream == null) {
                unionStream = stream;
            } else {
                unionStream = unionStream.union(stream);
            }
        }

        // 窗口聚合
        SingleOutputStreamOperator<PathTData> mergedPathTDataStream = unionStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PathTData>forBoundedOutOfOrderness(Duration.ofMillis(300))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<PathTData>) (pathData, recordTimestamp) -> pathData.getTime())
                                .withIdleness(Duration.ofSeconds(10)))
                .keyBy(PathTData::getSegId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.milliseconds(200)))
                .aggregate(new AggregateFunction<PathTData, PathTData, PathTData>() {
                    @Override
                    public PathTData createAccumulator() {
                        return new PathTData(0, 0L, "", null, new ArrayList<>());
                    }

                    @Override
                    public PathTData add(PathTData value, PathTData accumulator) {
                        if (accumulator.getTime() == 0L) {
                            accumulator.setTime(value.getTime());
                        }
                        if (accumulator.getTimeStamp().isEmpty()) {
                            accumulator.setTimeStamp(value.getTimeStamp());
                        }
                        accumulator.getPathList().addAll(value.getPathList());
                        accumulator.setPathNum(accumulator.getPathNum() + value.getPathNum());
                        return accumulator;
                    }

                    @Override
                    public PathTData getResult(PathTData accumulator) {
                        return accumulator;
                    }

                    @Override
                    public PathTData merge(PathTData a, PathTData b) {
                        a.getPathList().addAll(b.getPathList());
                        a.setPathNum(a.getPathNum() + b.getPathNum());
                        return a;
                    }
                });

        // ================== 创建输出Sink ==================
        KafkaSink<String> primarySink = KafkaSink.<String>builder()
                .setBootstrapServers(primaryBrokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("trajectoryoutput")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // ================== 处理合并后的数据流 ==================
        SingleOutputStreamOperator<String> primaryProcessed = mergedPathTDataStream
                .flatMap(new PrimaryTrajectoryProcessor())
                .name("Primary Trajectory Processor");

        // ================== 输出结果 ==================
        primaryProcessed.sinkTo(primarySink).name("Primary Output Sink");

        env.execute("Trajectory Analysis Job (NoFeature)");
    }

    // 时间戳解析方法
    private static long parseTimestamp(String timestampStr) {
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
            LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception ex) {
                return System.currentTimeMillis();
            }
        }
    }

    // 安全获取direction方法
    private static int getDirectionSafely(JSONObject tdata) {
        try {
            return tdata.getInt("direction");
        } catch (JSONException e) {
            return -1;
        }
    }

    // ================== 主数据处理逻辑 ==================
    private static class PrimaryTrajectoryProcessor implements FlatMapFunction<PathTData, String> {
        private static final long SESSION_TIMEOUT_MS = 10000;
        private static final long SAMPLING_INTERVAL_MS = 1000;

        // 状态存储
        private final Map<String, List<Tuple5<Double, Double, Integer, Integer, Double>>> map = new ConcurrentHashMap<>();
        private final Map<String, String> mapTimeSeg = new ConcurrentHashMap<>();
        private final Map<String, Integer> mapType = new ConcurrentHashMap<>();
        private final Map<String, Long> lastSeenTime = new ConcurrentHashMap<>();
        private final Map<String, Long> lastSampleTime = new ConcurrentHashMap<>();
        private final ReentrantLock stateLock = new ReentrantLock();

        @Override
        public void flatMap(PathTData pathData, Collector<String> out) {
            stateLock.lock();
            try {
                long timeObs = pathData.getTime();
                List<PathPoint> pathList = pathData.getPathList();

                for (PathPoint point : pathList) {
                    String plateNo = point.getPlateNo();
                    String id = String.valueOf(point.getId());

                    lastSeenTime.put(id, timeObs);
                    long lastSample = lastSampleTime.getOrDefault(id, 0L);

                    if (timeObs - lastSample >= SAMPLING_INTERVAL_MS) {
                        if (!map.containsKey(id)) {
                            initializeNewVehicle(id, plateNo, point, timeObs);
                        } else {
                            updateVehicleTrajectory(id, point);
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

        private void initializeNewVehicle(String id, String plateNo, PathPoint point, long timestamp) {
            mapTimeSeg.put(id, timestamp + "-" + plateNo + "-" + id);
            mapType.put(id, point.getVehicleType());

            List<Tuple5<Double, Double, Integer, Integer, Double>> list = new ArrayList<>();
            list.add(new Tuple5<>(
                    point.getLongitude(),
                    point.getLatitude(),
                    point.getLaneNo(),
                    point.getDirection(),
                    point.getSpeed()
            ));
            map.put(id, list);
        }

        private void updateVehicleTrajectory(String id, PathPoint point) {
            List<Tuple5<Double, Double, Integer, Integer, Double>> list = map.get(id);
            list.add(new Tuple5<>(
                    point.getLongitude(),
                    point.getLatitude(),
                    point.getLaneNo(),
                    point.getDirection(),
                    point.getSpeed()
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
    }
}