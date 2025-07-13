package whu.edu.moniData.ingest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
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

public class CarTrajAnalysisJobZa {
    private static final long SESSION_TIMEOUT_MS = 10000;
    private static final long SAMPLING_INTERVAL_MS = 1000;

    // 使用id作为键的线程安全Map
    private static final Map<String, List<Tuple5<Double, Double, Integer, Integer, Double>>> map = new ConcurrentHashMap<>();
    private static final Map<String, String> mapTimeSeg = new ConcurrentHashMap<>();
    private static final Map<String, Integer> mapType = new ConcurrentHashMap<>();
    private static final Map<String, Long> lastSeenTime = new ConcurrentHashMap<>();
    private static final Map<String, Long> lastSampleTime = new ConcurrentHashMap<>();

    // 全局锁
    private static final ReentrantLock stateLock = new ReentrantLock();

    private static int getDirectionSafely(JSONObject tdataObject) {
        try {
            return tdataObject.getInt("direction");
        } catch (JSONException e) {
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String brokers = "100.65.38.40:9092";
        String groupId = "flink-group";

        List<String> topics = Arrays.asList("MergedPathData");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics.get(0))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setProperty("consumer.max.poll.interval.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setProperty("session.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> unionStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source 1");

        for (int i = 1; i < topics.size(); i++) {
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics.get(i))
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setProperty("auto.offset.commit", "true")
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperty("consumer.max.poll.interval.ms", String.valueOf(24 * 60 * 60 * 1000))
                    .setProperty("session.timeout.ms", String.valueOf(24 * 60 * 60 * 1000))
                    .build();

            DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source " + (i + 1));
            unionStream = unionStream.union(stream);
        }

        // 创建Kafka Sink用于发送轨迹数据
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("zaOutPut")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        unionStream
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String jsonString, Collector<String> out) {
                        stateLock.lock();
                        try {
                            JSONObject jsonObject = new JSONObject(jsonString);
                            String timeStampStr = jsonObject.getString("timeStamp");
                            long timeObs;
                            try {
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                                LocalDateTime localDateTime = LocalDateTime.parse(timeStampStr, formatter);
                                timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            } catch (Exception e) {
                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                                LocalDateTime localDateTime = LocalDateTime.parse(timeStampStr, formatter);
                                timeObs = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            }

                            JSONArray tdataArray = jsonObject.getJSONArray("pathList");

                            // 处理轨迹数据
                            for (int i = 0; i < tdataArray.length(); i++) {
                                JSONObject tdataObject = tdataArray.getJSONObject(i);
                                // 获取车牌号和ID
                                String plateNo = tdataObject.getString("plateNo");
                                long idValue = tdataObject.getLong("id");
                                String id = String.valueOf(idValue);

                                // 更新状态
                                lastSeenTime.put(id, timeObs);
                                long lastSample = lastSampleTime.getOrDefault(id, 0L);

                                if (timeObs - lastSample >= SAMPLING_INTERVAL_MS) {
                                    // 如果该id不存在轨迹，初始化状态
                                    if (!map.containsKey(id)) {
                                        int type = tdataObject.getInt("vehicleType");
                                        // 新RowKey格式：时间戳-车牌号-id
                                        mapTimeSeg.put(id, timeObs + "-" + plateNo + "-" + id);
                                        mapType.put(id, type);
                                        List<Tuple5<Double, Double, Integer, Integer, Double>> list = new ArrayList<>();
                                        list.add(new Tuple5<>(tdataObject.getDouble("longitude"),
                                                tdataObject.getDouble("latitude"),
                                                tdataObject.getInt("laneNo"),
                                                getDirectionSafely(tdataObject),
                                                tdataObject.getDouble("speed")));
                                        map.put(id, list);
                                    } else {
                                        // 如果已存在轨迹，追加新的轨迹点
                                        List<Tuple5<Double, Double, Integer, Integer, Double>> list = map.get(id);
                                        list.add(new Tuple5<>(tdataObject.getDouble("longitude"),
                                                tdataObject.getDouble("latitude"),
                                                tdataObject.getInt("laneNo"),
                                                getDirectionSafely(tdataObject),
                                                tdataObject.getDouble("speed")));
                                    }
                                    lastSampleTime.put(id, timeObs);
                                }
                            }

                            // 安全地处理超时车辆
                            Set<String> timeoutIds = new HashSet<>();
                            for (Map.Entry<String, Long> entry : lastSeenTime.entrySet()) {
                                String id = entry.getKey();
                                long lastSeen = entry.getValue();
                                long de = timeObs - lastSeen;
                                if (de > SESSION_TIMEOUT_MS) {
                                    timeoutIds.add(id);
                                }
                            }

                            if (!timeoutIds.isEmpty()) {
                                for (String id : timeoutIds) {
                                    // 构建JSON格式的轨迹数据
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

                                    // 清理状态
                                    map.remove(id);
                                    mapTimeSeg.remove(id);
                                    mapType.remove(id);
                                    lastSeenTime.remove(id);
                                    lastSampleTime.remove(id);
                                }
                            }
                        } catch (Exception e) {
                            // 错误处理（可选）
                        } finally {
                            stateLock.unlock();
                        }
                    }
                })
                .sinkTo(kafkaSink);

        env.execute("Trajectory Analysis Job");
    }
}