package whu.edu.moniData;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class cunUPDOWNToKafka {

    static Map<String, String> bigIdToSmallId = new ConcurrentHashMap<>();
    static Map<String, String> stationNames = new ConcurrentHashMap<>();

    // 创建保留两位小数的格式化器
    private static final DecimalFormat df = new DecimalFormat("#.##");

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
        // 初始化站点配置
        bigIdToSmallId.put("XG01", "C7370151-2116-470A-8E26-5F878B3C9D78");
        stationNames.put("C7370151-2116-470A-8E26-5F878B3C9D78", "孝感收费站");

        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.48.53.82:9092");
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

        DataStream<String> inputStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 处理数据流，实时累积统计数据
        DataStream<String> realTimeStatsStream = inputStream
                .keyBy(json -> {
                    try {
                        JSONObject jsonObj = JSON.parseObject(json);
                        String bigOrgCode = jsonObj.getString("orgCode");
                        return bigIdToSmallId.getOrDefault(bigOrgCode, "unknown");
                    } catch (Exception e) {
                        return "parse_error";
                    }
                })
                .process(new RealTimeTrafficProcessFunction());

        // Kafka生产者配置
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("10.48.53.82:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("wd.platform.en.ex.vehicles")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setKafkaProducerConfig(producerProps)
                .build();
        // 发送到Kafka
        realTimeStatsStream.sinkTo(kafkaSink);

        env.execute("Up Down To Kafka");
    }

    private static class RealTimeTrafficProcessFunction
            extends KeyedProcessFunction<String, String, String> {

        // 存储累积的车道统计数据
        private transient MapState<Integer, Tuple4<Integer, Integer, Integer, Double>> laneStatsState;
        // 存储已处理的车辆ID
        private transient MapState<Integer, Boolean> processedVehicleIdsState;
        // 存储当前日期（用于每日清空）
        private transient ValueState<String> currentDateState;

        @Override
        public void open(Configuration parameters) {
            // 初始化状态
            MapStateDescriptor<Integer, Tuple4<Integer, Integer, Integer, Double>> laneStatsDesc =
                    new MapStateDescriptor<>(
                            "laneStats",
                            TypeInformation.of(Integer.class),
                            TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Integer, Double>>() {})
                    );

            MapStateDescriptor<Integer, Boolean> vehicleIdsDesc =
                    new MapStateDescriptor<>(
                            "processedVehicleIds",
                            TypeInformation.of(Integer.class),
                            TypeInformation.of(Boolean.class)
                    );

            ValueStateDescriptor<String> dateDesc =
                    new ValueStateDescriptor<>("currentDate", TypeInformation.of(String.class));

            laneStatsState = getRuntimeContext().getMapState(laneStatsDesc);
            processedVehicleIdsState = getRuntimeContext().getMapState(vehicleIdsDesc);
            currentDateState = getRuntimeContext().getState(dateDesc);
        }

        @Override
        public void processElement(String jsonString, Context ctx, Collector<String> out) throws Exception {
            JSONObject jsonObj = JSON.parseObject(jsonString);
            String bigOrgCode = jsonObj.getString("orgCode");
            String orgcode = bigIdToSmallId.getOrDefault(bigOrgCode, "unknown");

            if ("unknown".equals(orgcode)) {
                System.err.println("Unknown org code: " + bigOrgCode);
                return;
            }

            String globalTime = jsonObj.getString("globalTime");

            // 提取日期部分（yyyy-MM-dd）
            String eventDate = globalTime.substring(0, 10);
            String currentDate = currentDateState.value();

            // 检查日期变化（新的一天）
            if (currentDate == null) {
                currentDateState.update(eventDate);
            } else if (!currentDate.equals(eventDate)) {
                // 新的一天，清空状态
                laneStatsState.clear();
                processedVehicleIdsState.clear();
                currentDateState.update(eventDate);
            }

            // 处理目标列表
            JSONArray targetList = jsonObj.getJSONArray("targetList");
            if (targetList != null) {
                for (int i = 0; i < targetList.size(); i++) {
                    JSONObject target = targetList.getJSONObject(i);
                    Integer id = target.getInteger("id");
                    Integer lane = target.getInteger("lane");
                    Integer carType = target.getInteger("carType");
                    Double speed = target.getDouble("speed");
                    if (id == null || lane == null || carType == null || speed == null) {
                        continue;
                    }
                    // 检查车辆是否已处理
                    if (processedVehicleIdsState.contains(id)) {
                        continue;
                    }
                    processedVehicleIdsState.put(id, true);

                    // 更新车道统计数据
                    Tuple4<Integer, Integer, Integer, Double> stats = laneStatsState.get(lane);
                    if (stats == null) {
                        stats = new Tuple4<>(0, 0, 0, 0.0);
                    }

                    int busCount = stats.f0;
                    int trackCount = stats.f1;
                    int vehicleCount = stats.f2;
                    double totalSpeed = stats.f3;

                    if (isBus(carType)) busCount++;
                    if (isTrack(carType)) trackCount++;
                    vehicleCount++;
                    totalSpeed += speed;

                    laneStatsState.put(lane, new Tuple4<>(busCount, trackCount, vehicleCount, totalSpeed));

                    // 每处理一辆车就发送一次更新
                    sendRealTimeStats(orgcode, globalTime, out);
                }
            }
        }

        private void sendRealTimeStats(String orgcode, String timestamp, Collector<String> out) throws Exception {
            // 获取站点配置
            String stationName = stationNames.getOrDefault(orgcode, "Unknown Station");

            // 按方向分组统计
            Map<Integer, List<JSONObject>> directionData = new HashMap<>();
            // 用于按方向统计总车辆数和总速度
            Map<Integer, Double> directionTotalSpeeds = new HashMap<>();
            Map<Integer, Integer> directionTotalVehicles = new HashMap<>();

            // 遍历所有车道
            for (Integer lane : laneStatsState.keys()) {
                Tuple4<Integer, Integer, Integer, Double> stats = laneStatsState.get(lane);
                int busCount = stats.f0;
                int trackCount = stats.f1;
                int vehicleCount = stats.f2;
                double totalSpeed = stats.f3;

                int totalCount = busCount + trackCount;
                // 计算平均速度作为double类型保留
                double avgSpeed = vehicleCount > 0 ? totalSpeed / vehicleCount : 0.0;

                // 根据车道号判断方向
                int direction = (lane % 2 == 1) ? 1 : 2;

                // 创建车道数据对象 - 保存原始double类型的值
                JSONObject laneData = new JSONObject();
                laneData.put("lane", lane);
                laneData.put("total", totalCount);
                laneData.put("minibus", busCount);
                laneData.put("truck", trackCount);
                laneData.put("aveSpeed", avgSpeed); // 直接存储为double

                // 添加到方向分组
                directionData.computeIfAbsent(direction, k -> new ArrayList<>()).add(laneData);

                // 累加方向级别的总速度和总车辆数
                directionTotalSpeeds.merge(direction, totalSpeed, Double::sum);
                directionTotalVehicles.merge(direction, vehicleCount, Integer::sum);
            }

            // 构建输出结构
            JSONArray vehicleList = new JSONArray();
            for (Map.Entry<Integer, List<JSONObject>> entry : directionData.entrySet()) {
                int direction = entry.getKey();
                JSONObject directionDataObj = new JSONObject();
                directionDataObj.put("direction", direction);

                int total = 0;
                int minibus = 0;
                int truck = 0;

                JSONArray laneList = new JSONArray();
                for (JSONObject laneData : entry.getValue()) {
                    laneList.add(laneData);
                    total += laneData.getIntValue("total");
                    minibus += laneData.getIntValue("minibus");
                    truck += laneData.getIntValue("truck");
                }

                directionDataObj.put("total", total);
                directionDataObj.put("minibus", minibus);
                directionDataObj.put("truck", truck);

                // 计算方向平均速度
                int totalVehicles = directionTotalVehicles.getOrDefault(direction, 0);
                double directionAvgSpeed = totalVehicles > 0 ?
                        directionTotalSpeeds.getOrDefault(direction, 0.0) / totalVehicles : 0.0;

                directionDataObj.put("aveSpeed", directionAvgSpeed); // 直接存储为double
                directionDataObj.put("laneList", laneList);
                vehicleList.add(directionDataObj);
            }

            // 构建最终的统计对象
            JSONObject result = new JSONObject();
            result.put("timeStamp", timestamp);
            result.put("stationId", orgcode);
            result.put("stationName", stationName);
            result.put("vehicleList", vehicleList);

            out.collect(result.toJSONString());
        }
    }
}
