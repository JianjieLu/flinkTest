package whu.edu.moniData;


import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import javafx.util.Pair;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import whu.edu.ljj.flink.utils.myTools;

import com.alibaba.fastjson2.JSON;
import lombok.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import static whu.edu.ljj.flink.xiaohanying.Utils.*;
import static whu.edu.moniData.Utils.totalOps.putLine;

public class upDownToKafka {

    static Map<String, Integer> idTid = new ConcurrentHashMap<>();
    static Map<String, Boolean> firstInput = new ConcurrentHashMap<>();
    static Map<String, String> bigIdToSmallId = new ConcurrentHashMap<>();
    static int ii1;
    static int ii2;
    static Map<Integer, Pair<Integer,Integer>> mmap = new ConcurrentHashMap<>();
    static Configuration conf = HBaseConfiguration.create();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    // Kafka 配置
    private static final String KAFKA_BROKERS = "100.65.38.40:9092";
    private static final String OUTPUT_TOPIC = "traffic_stats_output";

    // 站点名称映射
    private static final Map<String, String> STATION_NAME_MAP = new ConcurrentHashMap<>();
    static {
        STATION_NAME_MAP.put("C7370151-2116-470A-8E26-5F878B3C9D78", "孝感收费站");
    }

    // 判断公交类型的方法
    private static boolean isBus(int vt) {
        return vt == 1 || vt == 3 || vt == 7 || vt == 15;
    }

    // 判断轨道类型的方法
    private static boolean isTrack(int vt) {
        return vt == 2 || vt == 10 || vt == 8 || vt == 11 || vt == 170 || vt == 171 || vt == 172 ||
                vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177;
    }

    public static void main(String[] args) throws Exception {
        String i1 = args[0];
        String i2 = args[1];
        ii1 = Integer.parseInt(i1);
        ii2 = Integer.parseInt(i2);
        bigIdToSmallId.put("XG01", "C7370151-2116-470A-8E26-5F878B3C9D78");
        idTid.put("C7370151-2116-470A-8E26-5F878B3C9D78", 8);
        firstInput.put("C7370151-2116-470A-8E26-5F878B3C9D78", true);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // Kafka配置
        String brokers = "100.65.38.40:9092";
        String groupId = "flink-group";
        List<String> topics = Arrays.asList("e1_data_XG01");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics.get(0))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
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
                    .build();

            DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source " + (i + 1));
            unionStream = unionStream.union(stream);
        }

        // 处理数据流并输出到Kafka
        DataStream<String> outputStream = unionStream
                .keyBy(json -> {
                    try {
                        JSONObject jsonObj = JSON.parseObject(json);
                        String bigOrgCode = jsonObj.getString("orgCode");
                        return bigIdToSmallId.getOrDefault(bigOrgCode, "unknown");
                    } catch (Exception e) {
                        return "parse_error";
                    }
                })
                .process(new TrafficStatsProcessor());

        // 配置Kafka生产者
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", KAFKA_BROKERS);
        producerProps.setProperty("transaction.timeout.ms", "60000");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                OUTPUT_TOPIC,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
                        return new ProducerRecord<>(
                                OUTPUT_TOPIC,
                                element.getBytes(StandardCharsets.UTF_8)
                        );
                    }
                },
                producerProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        // 添加Kafka Sink
        outputStream.addSink(kafkaProducer);

        env.execute("Real-time Traffic Statistics");
    }

    private static class TrafficStatsProcessor extends KeyedProcessFunction<String, String, String> {
        // 按车道存储统计信息
        private transient MapState<Integer, Tuple3<Integer, Integer, Integer>> laneStatsState; // 车道号 -> (busCount, trackCount, totalCount)
        private transient ValueState<String> lastTimeState;
        private transient ValueState<Integer> lastMinuteState;
        private transient MapState<Integer, Boolean> processedIdsState; // 用于车辆去重
        private transient ValueState<JSONObject> lastStatsState; // 存储上一次的统计结果

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            // 初始化状态
            MapStateDescriptor<Integer, Tuple3<Integer, Integer, Integer>> laneStatsDesc =
                    new MapStateDescriptor<>("laneStats", Types.INT,
                            Types.TUPLE(Types.INT, Types.INT, Types.INT));

            ValueStateDescriptor<String> timeDesc =
                    new ValueStateDescriptor<>("lastTime", Types.STRING);

            ValueStateDescriptor<Integer> minuteDesc =
                    new ValueStateDescriptor<>("lastMinute", Types.INT);

            // 使用 MapState 替代 SetState
            MapStateDescriptor<Integer, Boolean> processedIdsDesc =
                    new MapStateDescriptor<>("processedIds", Types.INT, Types.BOOLEAN);

            // 存储上一次的统计结果
            ValueStateDescriptor<JSONObject> lastStatsDesc =
                    new ValueStateDescriptor<>("lastStats", TypeInformation.of(JSONObject.class));

            laneStatsState = getRuntimeContext().getMapState(laneStatsDesc);
            lastTimeState = getRuntimeContext().getState(timeDesc);
            lastMinuteState = getRuntimeContext().getState(minuteDesc);
            processedIdsState = getRuntimeContext().getMapState(processedIdsDesc);
            lastStatsState = getRuntimeContext().getState(lastStatsDesc);
        }

        @Override
        public void processElement(String jsonString, Context ctx,
                                   Collector<String> out) throws Exception {
            try {
                JSONObject jsonObj = JSON.parseObject(jsonString);
                String bigOrgCode = jsonObj.getString("orgCode");
                String orgcode = bigIdToSmallId.getOrDefault(bigOrgCode, "unknown");

                if ("unknown".equals(orgcode)) {
                    System.err.println("Unknown orgcode: " + bigOrgCode);
                    return;
                }

                String thisTime = jsonObj.getString("globalTime");
                String timeKey = thisTime.substring(ii1, ii2);
                int myKey = Integer.parseInt(thisTime.substring(14, 16));
                Integer targetId = idTid.get(orgcode);

                // 初始化状态
                if (lastTimeState.value() == null) {
                    lastTimeState.update(timeKey);
                    lastMinuteState.update(myKey);
                    laneStatsState.clear();
                    processedIdsState.clear();
                    lastStatsState.update(new JSONObject());
                }

                Map<Integer, Pair<Integer,Integer>> tempMap = new ConcurrentHashMap<>();
                Map<Integer, Integer> originalTypeMap = new ConcurrentHashMap<>();

                // 处理 targetList
                JSONArray targetList = jsonObj.getJSONArray("targetList");
                if (targetList != null) {
                    for (int i = 0; i < targetList.size(); i++) {
                        JSONObject target = targetList.getJSONObject(i);
                        Integer station = target.getInteger("station");
                        int lane = target.getIntValue("lane");
                        Integer id = target.getInteger("id");
                        Integer originalType = target.getInteger("carType");

                        if (station.equals(targetId)) {
                            // 检查车辆是否已处理
                            if (!processedIdsState.contains(id)) {
                                tempMap.put(id, new Pair<>(station, lane));
                                originalTypeMap.put(id, originalType);
                                processedIdsState.put(id, true);
                            }
                        }
                    }
                }

                // 是否有新车
                boolean hasNewVehicle = false;

                // 更新车道统计
                for (Map.Entry<Integer, Pair<Integer,Integer>> entry : tempMap.entrySet()) {
                    int lane = entry.getValue().getValue();
                    int vehicleType = originalTypeMap.get(entry.getKey());

                    // 获取当前车道统计
                    Tuple3<Integer, Integer, Integer> currentStats = laneStatsState.get(lane);
                    if (currentStats == null) {
                        currentStats = Tuple3.of(0, 0, 0);
                    }

                    // 更新统计
                    int busCount = currentStats.f0;
                    int trackCount = currentStats.f1;
                    int totalCount = currentStats.f2;

                    // 只统计客车和货车
                    if (isBus(vehicleType)) {
                        busCount++;
                        totalCount++;
                        hasNewVehicle = true;
                    } else if (isTrack(vehicleType)) {
                        trackCount++;
                        totalCount++;
                        hasNewVehicle = true;
                    }
                    // 其他车辆不统计

                    laneStatsState.put(lane, Tuple3.of(busCount, trackCount, totalCount));
                }

                // 如果有新车，立即推送统计结果
                if (hasNewVehicle) {
                    pushStatsToKafka(ctx, orgcode, thisTime, out);
                }

                // 每分钟推送一次统计结果（即使没有新车）
                int storedMinuteKey = lastMinuteState.value();
                if (storedMinuteKey != myKey) {
                    pushStatsToKafka(ctx, orgcode, thisTime, out);
                    lastMinuteState.update(myKey);
                }

                // 每小时清空
                String storedTimeKey = lastTimeState.value();
                if (!timeKey.equals(storedTimeKey)) {
                    laneStatsState.clear();
                    processedIdsState.clear();
                    lastTimeState.update(timeKey);
                    lastStatsState.update(new JSONObject());
                }
            } catch (Exception e) {
                System.err.println("处理数据异常: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private void pushStatsToKafka(Context ctx, String orgcode, String timestamp, Collector<String> out) {
            try {
                // 创建方向1（上行）和方向2（下行）的数据结构
                JSONObject direction1 = new JSONObject();
                direction1.put("direction", 1);
                direction1.put("total", 0);
                direction1.put("minibus", 0);
                direction1.put("truck", 0);
                JSONArray laneList1 = new JSONArray();
                direction1.put("laneList", laneList1);

                JSONObject direction2 = new JSONObject();
                direction2.put("direction", 2);
                direction2.put("total", 0);
                direction2.put("minibus", 0);
                direction2.put("truck", 0);
                JSONArray laneList2 = new JSONArray();
                direction2.put("laneList", laneList2);

                // 遍历所有车道数据
                for (Integer lane : laneStatsState.keys()) {
                    Tuple3<Integer, Integer, Integer> stats = laneStatsState.get(lane);
                    int busCount = stats.f0;
                    int trackCount = stats.f1;
                    int totalCount = stats.f2;

                    // 创建车道数据对象
                    JSONObject laneData = new JSONObject();
                    laneData.put("lane", lane);
                    laneData.put("total", totalCount);
                    laneData.put("minibus", busCount);
                    laneData.put("truck", trackCount);

                    // 根据车道号判断方向（奇数车道为上行，偶数车道为下行）
                    if (lane % 2 == 1) {
                        // 累加方向统计数据
                        direction1.put("total", direction1.getIntValue("total") + totalCount);
                        direction1.put("minibus", direction1.getIntValue("minibus") + busCount);
                        direction1.put("truck", direction1.getIntValue("truck") + trackCount);
                        laneList1.add(laneData);
                    } else {
                        // 累加方向统计数据
                        direction2.put("total", direction2.getIntValue("total") + totalCount);
                        direction2.put("minibus", direction2.getIntValue("minibus") + busCount);
                        direction2.put("truck", direction2.getIntValue("truck") + trackCount);
                        laneList2.add(laneData);
                    }
                }

                // 创建车辆列表
                JSONArray vehicleList = new JSONArray();
                vehicleList.add(direction1);
                vehicleList.add(direction2);

                // 创建完整结果对象
                JSONObject result = new JSONObject();
                result.put("timeStamp", timestamp);
                result.put("stationId", orgcode);
                result.put("stationName", STATION_NAME_MAP.getOrDefault(orgcode, "未知站点"));
                result.put("vehicleList", vehicleList);

                // 转换为JSON字符串
                String jsonOutput = result.toJSONString();

                // 输出到Kafka
                out.collect(jsonOutput);

                // 保存当前状态
                lastStatsState.update(result);

                System.out.println("推送统计结果: " + jsonOutput);
            } catch (Exception e) {
                System.err.println("构建统计结果异常: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}