package whu.edu.moniData.ingest.holyAnalysisJob;

import lombok.Getter;
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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class CombinedCarTrajAnalysisJob_NoFeature_WithStatis_Minutes {

    // Kafka指标输出配置
    private static final String METRICS_BROKERS = "10.48.53.82:9092";
    private static final String MINUTELY_METRICS_TOPIC = "traffic_metrics_minutely";
    private static final String HOURLY_METRICS_TOPIC = "traffic_metrics_hourly";

    // 自由流速度（单位：米/秒），假设为120km/h
    private static final double FREEFLOW_SPEED = 120.0 * 1000 / 3600; // 120km/h -> m/s

    // 共享状态：按分组存储的时间二元组列表
    public static final Map<String, List<TimePair>> groupedTimePairs = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ================== Kafka 配置 ==================
        String primaryBrokers = "10.48.53.82:9092";
        String groupId = "flink-combined-group";

        List<String> primaryTopics = Arrays.asList("fiberData1", "fiberData2", "fiberData3",
                "fiberData4", "fiberData5", "fiberData6",
                "fiberData7", "fiberData8", "fiberData9",
                "fiberData10", "fiberData11");

        System.out.println("从以下Kafka主题读取主数据: " + primaryTopics);

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

        // ================== 创建输出Sink ==================
        // 指标输出Sink
        KafkaSink<String> minutelyMetricsSink = KafkaSink.<String>builder()
                .setBootstrapServers(METRICS_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(MINUTELY_METRICS_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        KafkaSink<String> hourlyMetricsSink = KafkaSink.<String>builder()
                .setBootstrapServers(METRICS_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(HOURLY_METRICS_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // ================== 处理主数据流 ==================
        SingleOutputStreamOperator<String> primaryProcessed = primaryStream
                .flatMap(new PrimaryTrajectoryProcessor())
                .name("Primary Trajectory Processor");

        // 处理指标流
        DataStream<String> metricsStream = primaryProcessed
                .flatMap(new MetricsCalculator())
                .name("Traffic Metrics Calculator");

        // 输出分钟级别指标到Kafka
        metricsStream.sinkTo(minutelyMetricsSink).name("Minutely Metrics Sink");
        System.out.println("分钟级别指标输出到Kafka主题: " + MINUTELY_METRICS_TOPIC);

        // 输出小时级别指标到Kafka
        metricsStream.sinkTo(hourlyMetricsSink).name("Hourly Metrics Sink");
        System.out.println("小时级别指标输出到Kafka主题: " + HOURLY_METRICS_TOPIC);

        env.execute("Minutes statistic (NoFeature)");
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

        // 计算范围内的车辆时间二元组
        private final Map<String, Map<String, TimePair>> vehicleTimePairs = new ConcurrentHashMap<>();

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

                    // 处理计算范围内的数据点
                    processInRangePoint(id, plateNo, tdataObject, timeObs);

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
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                stateLock.unlock();
            }
        }

        // 处理计算范围内的数据点
        private void processInRangePoint(String id, String plateNo, JSONObject tdata, long timestamp) {
            try {
                // 获取桩号（使用stakeId字段）
                int stakeMeters = tdata.getInt("mileage");

                // 检查是否在计算范围内
                if (isNearThousand(stakeMeters)) {
                    // 获取车道、方向
                    int laneNo = tdata.getInt("laneNo");
                    int direction = tdata.getInt("direction");

                    // 获取最近的桩号（整千米）
                    int roundedStake = (int) Math.round(stakeMeters / 1000.0) * 1000;

                    // 获取时间段（分钟）
                    String minute = getMinuteFromTimestamp(timestamp);

                    // 创建分组键 (格式: 车道_方向_桩号_分钟)
                    String groupKey = String.format("%d_%d_%d_%s", laneNo, direction, roundedStake, minute);

                    // 获取或创建车辆的时间二元组
                    Map<String, TimePair> vehicleMap = vehicleTimePairs.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
                    TimePair timePair = vehicleMap.computeIfAbsent(groupKey, k -> new TimePair());

                    // 更新时间二元组
                    if (timePair.firstTime == 0) {
                        timePair.firstTime = timestamp;
                    }
                    timePair.lastTime = timestamp;

                    // 更新分组的时间二元组列表（使用共享状态）
                    List<TimePair> groupList = groupedTimePairs.computeIfAbsent(groupKey, k -> new CopyOnWriteArrayList<>());

                    // 如果车辆在该分组中已有记录，则更新它
                    boolean found = false;
                    for (TimePair tp : groupList) {
                        if (tp.vehicleId.equals(id)) {
                            tp.firstTime = Math.min(tp.firstTime, timestamp);
                            tp.lastTime = Math.max(tp.lastTime, timestamp);
                            found = true;
                            break;
                        }
                    }

                    // 如果没有找到，则添加新记录
                    if (!found) {
                        groupList.add(new TimePair(id, plateNo, timePair.firstTime, timePair.lastTime));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 从时间戳获取分钟字符串
        private String getMinuteFromTimestamp(long timestamp) {
            LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            return dateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
        }

        private long parseTimestamp(String timestampStr) throws Exception {
            try {
                // 示例格式: "2025-08-03 14:44:21:271"
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception e) {
                // 尝试备用格式
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

        // 安全获取方法
        private int getDirectionSafely(JSONObject tdata) {
            try { return tdata.getInt("direction"); }
            catch (JSONException e) { return -1; }
        }

        // 判断是否在计算范围内
        public static boolean isNearThousand(int n) {
            // 计算最近的整一千数（考虑四舍五入）
            long roundedThousand;
            if (n >= 0) {
                roundedThousand = Math.round(n / 1000.0) * 1000;
            } else {
                // 对于负数，需要特殊处理四舍五入
                roundedThousand = Math.round(n / 1000.0) * 1000;
            }

            // 计算绝对差值
            long diff = Math.abs(n - roundedThousand);

            // 判断差值是否在12以内
            return diff <= 12;
        }
    }

    // 时间二元组类
    private static class TimePair {
        String vehicleId;
        String plateNo;
        long firstTime; // 第一次出现在计算范围内的时间
        long lastTime;  // 最近一次出现在计算范围内的时间

        public TimePair() {
            this("", "", 0, 0);
        }

        public TimePair(String vehicleId, String plateNo, long firstTime, long lastTime) {
            this.vehicleId = vehicleId;
            this.plateNo = plateNo;
            this.firstTime = firstTime;
            this.lastTime = lastTime;
        }

        // 获取时间差（毫秒）
        public long getDuration() {
            return lastTime - firstTime;
        }
    }

    // 指标计算器（使用真实数据）
    private static class MetricsCalculator implements FlatMapFunction<String, String> {
        // 存储每小时的指标（按分组键）
        private final Map<String, AggregatedMetrics> hourlyMetricsMap = new ConcurrentHashMap<>();
        private long lastMinuteCalculated = 0;

        @Override
        public void flatMap(String value, Collector<String> out) {
            // 每分钟触发一次计算
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastMinuteCalculated >= 60000) { // 每分钟检查一次
                calculateMinuteMetrics(currentTime, out);
                lastMinuteCalculated = currentTime;
            }
        }

        private void calculateMinuteMetrics(long currentTime, Collector<String> out) {
            // 获取前一分钟
            String minute = getMinuteFromTimestamp(currentTime - 60000);

            // 遍历所有分组键
            for (Map.Entry<String, List<TimePair>> entry : groupedTimePairs.entrySet()) {
                String groupKey = entry.getKey();
                String[] parts = groupKey.split("_");
                if (parts.length < 4) continue;

                String groupMinute = parts[3];
                if (!groupMinute.equals(minute)) continue;

                // 提取分组信息
                int laneNo = Integer.parseInt(parts[0]);
                int direction = Integer.parseInt(parts[1]);
                int roundedStake = Integer.parseInt(parts[2]);

                // 计算该分组的指标
                MinuteMetrics minuteMetrics = calculateMetricsForGroup(entry.getValue());

                if (minuteMetrics != null) {
                    // 输出分组指标
                    JSONObject minutelyJson = new JSONObject();
                    minutelyJson.put("time", minute);
                    minutelyJson.put("laneNo", laneNo);
                    minutelyJson.put("direction", direction);
                    minutelyJson.put("roundedStake", roundedStake);
                    minutelyJson.put("occupancy", minuteMetrics.occupancy);
                    minutelyJson.put("headway", minuteMetrics.headway);
                    minutelyJson.put("delay_index", minuteMetrics.delayIndex);
                    minutelyJson.put("vehicle_count", minuteMetrics.vehicleCount);

                    out.collect(minutelyJson.toString());

                    // 更新小时聚合指标
                    updateHourlyMetrics(minute, laneNo, direction, roundedStake, minuteMetrics, out);
                }
            }
        }

        // 计算指定分组的指标
        private MinuteMetrics calculateMetricsForGroup(List<TimePair> timePairs) {
            if (timePairs.isEmpty()) {
                return null;
            }

            // 1. 时间占有率
            double totalDuration = 0;
            for (TimePair tp : timePairs) {
                totalDuration += tp.getDuration();
            }
            double occupancy = totalDuration / (60 * 1000.0); // 转换为秒并除以60秒

            // 2. 计算车头时距
            // 按驶入时间排序
            timePairs.sort(Comparator.comparingLong(tp -> tp.firstTime));

            double totalHeadway = 0;
            int headwayCount = 0;
            for (int i = 1; i < timePairs.size(); i++) {
                long headway = timePairs.get(i).firstTime - timePairs.get(i - 1).firstTime;
                totalHeadway += headway;
                headwayCount++;
            }
            double avgHeadway = headwayCount > 0 ? totalHeadway / headwayCount : 0;

            // 3. 计算车辆延时指数
            double totalActualTime = 0;
            double totalFreeFlowTime = 0;
            for (TimePair tp : timePairs) {
                totalActualTime += tp.getDuration();
                // 自由流通过时间 = 距离 / 速度 = 24米 / 自由流速度
                totalFreeFlowTime += 24.0 / FREEFLOW_SPEED * 1000; // 转换为毫秒
            }
            double delayIndex = totalActualTime / totalFreeFlowTime;

            return new MinuteMetrics(occupancy, avgHeadway, delayIndex, timePairs.size());
        }

        // 更新小时聚合指标
        private void updateHourlyMetrics(String minute, int laneNo, int direction, int roundedStake,
                                         MinuteMetrics minuteMetrics, Collector<String> out) {
            // 解析时间
            int year = Integer.parseInt(minute.substring(0, 4));
            int month = Integer.parseInt(minute.substring(4, 6));
            int day = Integer.parseInt(minute.substring(6, 8));
            int hour = Integer.parseInt(minute.substring(8, 10));

            // 创建小时分组键 (格式: 车道_方向_桩号_小时)
            String hourKey = String.format("%d_%d_%d_%04d%02d%02d%02d",
                    laneNo, direction, roundedStake, year, month, day, hour);

            // 更新小时指标
            AggregatedMetrics hourMetrics = hourlyMetricsMap.computeIfAbsent(hourKey, k -> new AggregatedMetrics());
            hourMetrics.addMinuteMetrics(minuteMetrics);

            // 检查是否是新的一小时开始（分钟为00）
            if (minute.endsWith("00")) {
                // 获取上一个小时的键
                LocalDateTime dateTime = LocalDateTime.parse(minute + "00", DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                LocalDateTime previousHour = dateTime.minusHours(1);
                String previousHourKey = String.format("%d_%d_%d_%s",
                        laneNo, direction, roundedStake,
                        previousHour.format(DateTimeFormatter.ofPattern("yyyyMMddHH")));

                // 获取并移除上一个小时的聚合指标
                AggregatedMetrics previousHourMetrics = hourlyMetricsMap.remove(previousHourKey);
                if (previousHourMetrics != null) {
                    System.out.println("计算小时指标: " + previousHourKey +
                            ", 平均占有率: " + previousHourMetrics.getAvgOccupancy() +
                            ", 平均车头时距: " + previousHourMetrics.getAvgHeadway() +
                            ", 平均延时指数: " + previousHourMetrics.getAvgDelayIndex() +
                            ", 总车辆数: " + previousHourMetrics.getTotalVehicleCount());

                    // 计算小时指标
                    JSONObject hourlyJson = new JSONObject();
                    hourlyJson.put("time", previousHourKey);
                    hourlyJson.put("avg_occupancy", previousHourMetrics.getAvgOccupancy());
                    hourlyJson.put("avg_headway", previousHourMetrics.getAvgHeadway());
                    hourlyJson.put("avg_delay_index", previousHourMetrics.getAvgDelayIndex());
                    hourlyJson.put("total_vehicles", previousHourMetrics.getTotalVehicleCount());

                    out.collect(hourlyJson.toString());
                }
            }
        }

        // 从时间戳获取分钟字符串
        private String getMinuteFromTimestamp(long timestamp) {
            LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            return dateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
        }
    }

    // 每分钟指标类
    private static class MinuteMetrics {
        double occupancy;    // 时间占有率
        double headway;      // 平均车头时距（毫秒）
        double delayIndex;   // 车辆延时指数
        int vehicleCount;    // 车辆数

        public MinuteMetrics(double occupancy, double headway, double delayIndex, int vehicleCount) {
            this.occupancy = occupancy;
            this.headway = headway;
            this.delayIndex = delayIndex;
            this.vehicleCount = vehicleCount;
        }
    }

    // 聚合指标类（用于小时）
    @Getter
    private static class AggregatedMetrics {
        double totalOccupancy = 0;
        double totalHeadway = 0;
        double totalDelayIndex = 0;
        int totalVehicleCount = 0;
        int minuteCount = 0;

        public void addMinuteMetrics(MinuteMetrics metrics) {
            totalOccupancy += metrics.occupancy;
            totalHeadway += metrics.headway;
            totalDelayIndex += metrics.delayIndex;
            totalVehicleCount += metrics.vehicleCount;
            minuteCount++;
        }

        public double getAvgOccupancy() {
            return minuteCount > 0 ? totalOccupancy / minuteCount : 0;
        }

        public double getAvgHeadway() {
            return minuteCount > 0 ? totalHeadway / minuteCount : 0;
        }

        public double getAvgDelayIndex() {
            return minuteCount > 0 ? totalDelayIndex / minuteCount : 0;
        }
    }
}