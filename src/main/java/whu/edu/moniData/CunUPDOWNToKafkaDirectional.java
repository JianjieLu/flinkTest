package whu.edu.moniData;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Properties;

public class CunUPDOWNToKafkaDirectional {

    static Map<String, Integer> idTid = new ConcurrentHashMap<>();
    static Map<String, Boolean> firstInput = new ConcurrentHashMap<>();
    static Map<String, String> bigIdToSmallId = new ConcurrentHashMap<>();
    static Map<String, String> stationNames = new ConcurrentHashMap<>();
    static Map<String, Map<Integer, Integer>> laneDirections = new ConcurrentHashMap<>();
    static int ii1;
    static int ii2;
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
    private static final DateTimeFormatter OUTPUT_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Kafka生产者配置
    private static final String OUTPUT_BROKERS = "10.48.53.82:9092";
    private static final String OUTPUT_TOPIC = "wd.platform.en.ex.vehicles";

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

        // 初始化站点配置
        bigIdToSmallId.put("XG01", "C7370151-2116-470A-8E26-5F878B3C9D78");
        stationNames.put("C7370151-2116-470A-8E26-5F878B3C9D78", "孝感收费站");
        idTid.put("C7370151-2116-470A-8E26-5F878B3C9D78", 8);
        firstInput.put("C7370151-2116-470A-8E26-5F878B3C9D78", true);

        // 初始化车道方向配置
        Map<Integer, Integer> xg01Directions = new HashMap<>();
        xg01Directions.put(1, 1); // 车道1 -> 方向1(上行)
        xg01Directions.put(3, 1); // 车道3 -> 方向1(上行)
        xg01Directions.put(5, 1); // 车道5 -> 方向1(上行)
        xg01Directions.put(2, 2); // 车道2 -> 方向2(下行)
        xg01Directions.put(4, 2); // 车道4 -> 方向2(下行)
        xg01Directions.put(6, 2); // 车道6 -> 方向2(下行)
        laneDirections.put("C7370151-2116-470A-8E26-5F878B3C9D78", xg01Directions);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // Kafka配置 - 输入源
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

        // 输出为JSON字符串
        DataStream<String> jsonStream = unionStream
                .keyBy(json -> {
                    try {
                        JSONObject jsonObj = JSON.parseObject(json);
                        String bigOrgCode = jsonObj.getString("orgCode");
                        return bigIdToSmallId.getOrDefault(bigOrgCode, "unknown");
                    } catch (Exception e) {
                        return "parse_error";
                    }
                })
                .process(new KeyedProcessFunction<String, String, String>() {
                    // 按车道存储统计信息
                    private transient MapState<Integer, Tuple5<Integer, Integer, Integer, Double, Integer>> laneStatsState;
                    private transient ValueState<String> lastTimeState;
                    private transient ValueState<Integer> lastMinuteState;
                    private transient MapState<Integer, Boolean> processedIdsState;
                    private transient ValueState<String> lastGlobalTimeState;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) {
                        // 初始化状态
                        MapStateDescriptor<Integer, Tuple5<Integer, Integer, Integer, Double, Integer>> laneStatsDesc =
                                new MapStateDescriptor<>("laneStats", Types.INT,
                                        Types.TUPLE(Types.INT, Types.INT, Types.INT, Types.DOUBLE, Types.INT));

                        ValueStateDescriptor<String> timeDesc =
                                new ValueStateDescriptor<>("lastTime", Types.STRING);

                        ValueStateDescriptor<Integer> minuteDesc =
                                new ValueStateDescriptor<>("lastMinute", Types.INT);

                        MapStateDescriptor<Integer, Boolean> processedIdsDesc =
                                new MapStateDescriptor<>("processedIds", Types.INT, Types.BOOLEAN);

                        ValueStateDescriptor<String> globalTimeDesc =
                                new ValueStateDescriptor<>("lastGlobalTime", Types.STRING);

                        laneStatsState = getRuntimeContext().getMapState(laneStatsDesc);
                        lastTimeState = getRuntimeContext().getState(timeDesc);
                        lastMinuteState = getRuntimeContext().getState(minuteDesc);
                        processedIdsState = getRuntimeContext().getMapState(processedIdsDesc);
                        lastGlobalTimeState = getRuntimeContext().getState(globalTimeDesc);
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

                            // 更新全局时间状态
                            lastGlobalTimeState.update(thisTime);

                            // 初始化状态
                            if (lastTimeState.value() == null) {
                                lastTimeState.update(timeKey);
                                lastMinuteState.update(myKey);
                                laneStatsState.clear();
                                processedIdsState.clear();
                            }

                            Map<Integer, Pair<Integer, Integer>> tempMap = new ConcurrentHashMap<>();
                            Map<Integer, Integer> originalTypeMap = new ConcurrentHashMap<>();
                            Map<Integer, Double> speedMap = new ConcurrentHashMap<>();

                            // 处理 targetList
                            JSONArray targetList = jsonObj.getJSONArray("targetList");
                            if (targetList != null) {
                                for (int i = 0; i < targetList.size(); i++) {
                                    JSONObject target = targetList.getJSONObject(i);
                                    Integer station = target.getInteger("station");
                                    int lane = target.getIntValue("lane");
                                    Integer id = target.getInteger("id");
                                    Integer originalType = target.getInteger("carType");
                                    double speed = target.getDoubleValue("speed");

                                    if (station.equals(targetId)) {
                                        // 检查车辆是否已处理
                                        if (!processedIdsState.contains(id)) {
                                            tempMap.put(id, new Pair<>(station, lane));
                                            originalTypeMap.put(id, originalType);
                                            speedMap.put(id, speed);
                                            processedIdsState.put(id, true);
                                        }
                                    }
                                }
                            }

                            // 更新车道统计
                            for (Map.Entry<Integer, Pair<Integer, Integer>> entry : tempMap.entrySet()) {
                                int lane = entry.getValue().getValue();
                                int vehicleType = originalTypeMap.get(entry.getKey());
                                double speed = speedMap.get(entry.getKey());

                                // 只处理公交和轨道车辆
                                if (isBus(vehicleType) || isTrack(vehicleType)) {
                                    // 获取当前车道统计
                                    Tuple5<Integer, Integer, Integer, Double, Integer> currentStats = laneStatsState.get(lane);
                                    if (currentStats == null) {
                                        currentStats = Tuple5.of(0, 0, 0, 0.0, 0);
                                    }

                                    // 更新统计
                                    int busCount = currentStats.f0;
                                    int trackCount = currentStats.f1;
                                    int totalCount = currentStats.f2;
                                    double totalSpeed = currentStats.f3;
                                    int vehicleCount = currentStats.f4;

                                    if (isBus(vehicleType)) {
                                        busCount++;
                                        totalCount++;
                                    } else if (isTrack(vehicleType)) {
                                        trackCount++;
                                        totalCount++;
                                    }

                                    // 累加速度并增加车辆计数
                                    totalSpeed += speed;
                                    vehicleCount++;

                                    laneStatsState.put(lane, Tuple5.of(busCount, trackCount, totalCount, totalSpeed, vehicleCount));
                                }
                            }

                            // 每分钟发射统计结果
                            int storedMinuteKey = lastMinuteState.value();
                            if (storedMinuteKey != myKey) {
                                System.out.println("storedMinuteKey:" + storedMinuteKey + "  myKey:" + myKey);

                                // 获取站点名称和车道方向映射
                                String stationName = stationNames.getOrDefault(orgcode, "Unknown Station");
                                Map<Integer, Integer> directionMap = laneDirections.get(orgcode);

                                if (directionMap != null) {
                                    // 构建方向分组数据
                                    Map<Integer, JSONObject> directionStats = new HashMap<>();
                                    Map<Integer, JSONArray> directionLaneList = new HashMap<>();

                                    // 初始化每个方向的数据结构
                                    for (int direction : directionMap.values()) {
                                        if (!directionStats.containsKey(direction)) {
                                            JSONObject dirObj = new JSONObject();
                                            dirObj.put("direction", direction);
                                            dirObj.put("total", 0);
                                            dirObj.put("minibus", 0);
                                            dirObj.put("truck", 0);
                                            dirObj.put("totalSpeed", 0.0);
                                            dirObj.put("vehicleCount", 0);
                                            directionStats.put(direction, dirObj);
                                            directionLaneList.put(direction, new JSONArray());
                                        }
                                    }

                                    // 遍历所有车道，按方向分组
                                    for (Integer lane : laneStatsState.keys()) {
                                        Tuple5<Integer, Integer, Integer, Double, Integer> stats = laneStatsState.get(lane);
                                        int busCount = stats.f0;
                                        int trackCount = stats.f1;
                                        int totalCount = stats.f2;
                                        double totalSpeed = stats.f3;
                                        int vehicleCount = stats.f4;

                                        // 计算车道平均速度
                                        double avgSpeed = vehicleCount > 0 ? totalSpeed / vehicleCount : 0.0;
                                        avgSpeed = Math.round(avgSpeed * 100.0) / 100.0;

                                        // 创建车道JSON
                                        JSONObject laneObj = new JSONObject();
                                        laneObj.put("lane", lane);
                                        laneObj.put("total", totalCount);
                                        laneObj.put("minibus", busCount);
                                        laneObj.put("truck", trackCount);
                                        laneObj.put("aveSpeed", avgSpeed);

                                        // 获取车道方向
                                        Integer direction = directionMap.get(lane);
                                        if (direction != null) {
                                            // 添加到方向的车道列表
                                            directionLaneList.get(direction).add(laneObj);

                                            // 更新方向的总统计
                                            JSONObject dirObj = directionStats.get(direction);
                                            dirObj.put("total", dirObj.getIntValue("total") + totalCount);
                                            dirObj.put("minibus", dirObj.getIntValue("minibus") + busCount);
                                            dirObj.put("truck", dirObj.getIntValue("truck") + trackCount);
                                            dirObj.put("totalSpeed", dirObj.getDoubleValue("totalSpeed") + totalSpeed);
                                            dirObj.put("vehicleCount", dirObj.getIntValue("vehicleCount") + vehicleCount);
                                        }
                                    }

                                    // 构建方向列表
                                    JSONArray vehicleList = new JSONArray();
                                    for (Map.Entry<Integer, JSONObject> entry : directionStats.entrySet()) {
                                        int direction = entry.getKey();
                                        JSONObject dirObj = entry.getValue();

                                        // 计算方向平均速度
                                        int dirVehicleCount = dirObj.getIntValue("vehicleCount");
                                        double dirTotalSpeed = dirObj.getDoubleValue("totalSpeed");
                                        double dirAvgSpeed = dirVehicleCount > 0 ? dirTotalSpeed / dirVehicleCount : 0.0;
                                        dirAvgSpeed = Math.round(dirAvgSpeed * 100.0) / 100.0;

                                        dirObj.put("aveSpeed", dirAvgSpeed);
                                        dirObj.put("laneList", directionLaneList.get(direction));

                                        // 移除临时字段
                                        dirObj.remove("totalSpeed");
                                        dirObj.remove("vehicleCount");

                                        vehicleList.add(dirObj);
                                    }

                                    // 构建最终结果
                                    JSONObject result = new JSONObject();
                                    result.put("stationId", orgcode);
                                    result.put("stationName", stationName);

                                    // 格式化时间戳
                                    String globalTime = lastGlobalTimeState.value();
                                    LocalDateTime timestamp = LocalDateTime.parse(globalTime, TIME_FORMATTER);
                                    result.put("timeStamp", timestamp.format(OUTPUT_TIME_FORMATTER));

                                    result.put("vehicleList", vehicleList);

                                    // 输出JSON字符串
                                    out.collect(result.toJSONString());
                                }

                                lastMinuteState.update(myKey);
                            }

                            // 每小时清空
                            String storedTimeKey = lastTimeState.value();
                            if (!timeKey.equals(storedTimeKey)) {
                                laneStatsState.clear();
                                processedIdsState.clear();
                                lastTimeState.update(timeKey);
                            }
                        } catch (Exception e) {
                            System.err.println("处理数据异常: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }

                    private long parseTimestamp(String timeString) throws DateTimeParseException {
                        return LocalDateTime.parse(timeString, TIME_FORMATTER)
                                .atZone(ZoneId.systemDefault())
                                .toInstant()
                                .toEpochMilli();
                    }
                });

        // 配置Kafka生产者属性
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, OUTPUT_BROKERS);
        producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        // 创建KafkaSink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(OUTPUT_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(OUTPUT_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(producerProps)
                .build();

        // 添加Kafka Sink
        jsonStream.sinkTo(kafkaSink)
                .name("Kafka Sink")
                .setParallelism(2);

        env.execute("Flink Directional Traffic Statistics to Kafka");
    }
}