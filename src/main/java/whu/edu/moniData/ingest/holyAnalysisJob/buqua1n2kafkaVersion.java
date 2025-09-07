package whu.edu.moniData.ingest.holyAnalysisJob;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import whu.edu.ljj.flink.xiaohanying.Utils.PathPoint;
import whu.edu.ljj.flink.xiaohanying.Utils.PathTData;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class buqua1n2kafkaVersion {
    // 自由流速度 (120km/h -> m/s)
    private static final double FREEFLOW_SPEED = 120.0 * 1000 / 3600;
    private static final int STAKE_RANGE = 12;

    // 复合键类
    static class CompositeKey {
        private String timeKey;
        private int laneNo;
        private int direction;
        private int stake;

        public CompositeKey(String timeKey, int laneNo, int direction, int stake) {
            this.timeKey = timeKey;
            this.laneNo = laneNo;
            this.direction = direction;
            this.stake = stake;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompositeKey that = (CompositeKey) o;
            return laneNo == that.laneNo &&
                    stake == that.stake &&
                    direction == that.direction &&
                    Objects.equals(timeKey, that.timeKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timeKey, laneNo, direction, stake);
        }

        public String getTimeKey() { return timeKey; }
        public int getLaneNo() { return laneNo; }
        public int getDirection() { return direction; }
        public int getStake() { return stake; }

        @Override
        public String toString() {
            return timeKey + "_" + laneNo + "_" + direction + "_" + stake;
        }
    }

    // 指标结果类
    static class IndicatorResult {
        private String timeKey;
        private String timeType; // "minute", "hour", "day", "month"
        private int laneNo;
        private int direction;
        private int stake;
        private double occupancy;
        private double headway;
        private double delayIndex;
        private int vehicleCount;

        public IndicatorResult(String timeKey, String timeType, int laneNo, int direction, int stake,
                               double occupancy, double headway, double delayIndex, int vehicleCount) {
            this.timeKey = timeKey;
            this.timeType = timeType;
            this.laneNo = laneNo;
            this.direction = direction;
            this.stake = stake;
            this.occupancy = occupancy;
            this.headway = headway;
            this.delayIndex = delayIndex;
            this.vehicleCount = vehicleCount;
        }

        // Getter方法
        public String getTimeKey() { return timeKey; }
        public String getTimeType() { return timeType; }
        public int getLaneNo() { return laneNo; }
        public int getDirection() { return direction; }
        public int getStake() { return stake; }
        public double getOccupancy() { return occupancy; }
        public double getHeadway() { return headway; }
        public double getDelayIndex() { return delayIndex; }
        public int getVehicleCount() { return vehicleCount; }

        @Override
        public String toString() {
            return String.format("%s,%s,%d,%d,%d,%.4f,%.4f,%.4f,%d",
                    timeKey, timeType, laneNo, direction, stake,
                    occupancy, headway, delayIndex, vehicleCount);
        }
    }

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);

        String brokers = "10.48.53.82:9092";
        List<String> topics = Arrays.asList(
                "fiberData1", "fiberData2", "fiberData3", "fiberData4", "fiberData5",
                "fiberData6", "fiberData7", "fiberData8", "fiberData9", "fiberData10", "fiberData11"
        );
        String groupId = "flink_consumer_group1";
        String minuteTopic = "traffic_indicators_minute";
        String hourTopic = "traffic_indicators_hour";
        String dayTopic = "traffic_indicators_day";
        String monthTopic = "traffic_indicators_month";

        // 创建Kafka源
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("max.partition.fetch.bytes", "629145600")
                .build();

        // 从Kafka读取数据
        DataStreamSource<String> kafkaStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // 解析JSON数据
        SingleOutputStreamOperator<PathTData> stationStream = kafkaStream
                .flatMap((String jsonStr, Collector<PathTData> out) -> {
                    try {
                        PathTData data = JSON.parseObject(jsonStr, PathTData.class);
                        if (data != null && data.getPathList() != null) {
                            out.collect(data);
                        }
                    } catch (Exception e) {
                        System.err.println("JSON解析失败: " + e.getMessage());
                    }
                }).returns(PathTData.class);

        // 计算分钟级指标
        SingleOutputStreamOperator<IndicatorResult> minuteIndicatorStream = stationStream
                .keyBy(PathTData::getTime)
                .flatMap(new RichFlatMapFunction<PathTData, IndicatorResult>() {
                    // 本地存储时间对数据
                    private transient Map<CompositeKey, List<Pair<Long, Long>>> timeDataMap;
                    // 记录当前处理的最新分钟
                    private transient String currentProcessingMinute;

                    @Override
                    public void open(Configuration parameters) {
                        timeDataMap = new ConcurrentHashMap<>();
                        currentProcessingMinute = "";
                    }

                    @Override
                    public void flatMap(PathTData pathTData, Collector<IndicatorResult> collector) throws Exception {
                        List<PathPoint> pathList = pathTData.getPathList();
                        String timestampStr = pathTData.getTimeStamp();
                        long timestamp = convertToTimestampMillis(timestampStr);
                        String minute = getMinuteFromTimestamp(timestamp);

                        // 如果是新的分钟，处理上一分钟的数据
                        if (!currentProcessingMinute.equals("") && !currentProcessingMinute.equals(minute)) {
                            processMinuteData(currentProcessingMinute, collector);
                        }

                        currentProcessingMinute = minute;

                        // 处理每个路径点
                        for (PathPoint point : pathList) {
                            int laneNo = point.getLaneNo();
                            int direction = point.getDirection();
                            Pair<Boolean, Integer> stakeResult = isNearThousand(point.getMileage());

                            if (stakeResult.getKey()) {
                                int stake = stakeResult.getValue();
                                CompositeKey key = new CompositeKey(minute, laneNo, direction, stake);

                                // 获取或创建时间对列表
                                List<Pair<Long, Long>> timePairs = timeDataMap.getOrDefault(key, new ArrayList<>());

                                // 这里简化处理：使用相同的时间作为进入和离开时间
                                // 实际应用中应该根据车辆轨迹计算准确的进入和离开时间
                                timePairs.add(new Pair<>(timestamp, timestamp));
                                timeDataMap.put(key, timePairs);
                            }
                        }
                    }

                    // 处理指定分钟的数据并清空
                    private void processMinuteData(String minute, Collector<IndicatorResult> collector) {
                        // 收集所有需要处理的键
                        List<CompositeKey> keysToProcess = new ArrayList<>();
                        for (CompositeKey key : timeDataMap.keySet()) {
                            if (key.getTimeKey().equals(minute)) {
                                keysToProcess.add(key);
                            }
                        }

                        // 处理每个键对应的数据
                        for (CompositeKey key : keysToProcess) {
                            List<Pair<Long, Long>> timePairs = timeDataMap.get(key);
                            if (timePairs != null && timePairs.size() > 0) {
                                // 计算指标
                                double occupancy = calculateOccupancy(timePairs);
                                double headway = calculateHeadway(timePairs);
                                double delayIndex = calculateDelayIndex(timePairs);
                                int vehicleCount = timePairs.size();

                                // 发出指标结果
                                collector.collect(new IndicatorResult(
                                        key.getTimeKey(), "minute", key.getLaneNo(), key.getDirection(), key.getStake(),
                                        occupancy, headway, delayIndex, vehicleCount
                                ));

                                // 清空已处理的数据
                                timePairs.clear();
                                timeDataMap.remove(key);
                            }
                        }
                    }

                    @Override
                    public void close() {
                        // 处理剩余数据（最后一分钟的数据）
                        if (!currentProcessingMinute.equals("")) {
                            // 这里需要实现最终指标计算逻辑
                            // 在实际应用中，可能需要将剩余数据保存或发送到其他地方处理
                        }
                    }
                });

        // 创建小时级聚合
        SingleOutputStreamOperator<IndicatorResult> hourIndicatorStream = minuteIndicatorStream
                .keyBy(result -> new CompositeKey(
                        getHourFromMinute(result.getTimeKey()),
                        result.getLaneNo(),
                        result.getDirection(),
                        result.getStake()
                ))
                .flatMap(new RichFlatMapFunction<IndicatorResult, IndicatorResult>() {
                    private transient Map<CompositeKey, List<IndicatorResult>> hourDataMap;
                    private transient String currentProcessingHour;

                    @Override
                    public void open(Configuration parameters) {
                        hourDataMap = new ConcurrentHashMap<>();
                        currentProcessingHour = "";
                    }

                    @Override
                    public void flatMap(IndicatorResult minuteResult, Collector<IndicatorResult> collector) throws Exception {
                        String hourKey = getHourFromMinute(minuteResult.getTimeKey());

                        // 如果是新的小时，处理上一小时的数据
                        if (!currentProcessingHour.equals("") && !currentProcessingHour.equals(hourKey)) {
                            processHourData(currentProcessingHour, collector);
                        }

                        currentProcessingHour = hourKey;

                        // 存储分钟数据
                        CompositeKey key = new CompositeKey(
                                hourKey,
                                minuteResult.getLaneNo(),
                                minuteResult.getDirection(),
                                minuteResult.getStake()
                        );

                        List<IndicatorResult> hourData = hourDataMap.getOrDefault(key, new ArrayList<>());
                        hourData.add(minuteResult);
                        hourDataMap.put(key, hourData);
                    }

                    // 处理指定小时的数据并清空
                    private void processHourData(String hourKey, Collector<IndicatorResult> collector) {
                        // 收集所有需要处理的键
                        List<CompositeKey> keysToProcess = new ArrayList<>();
                        for (CompositeKey key : hourDataMap.keySet()) {
                            if (key.getTimeKey().equals(hourKey)) {
                                keysToProcess.add(key);
                            }
                        }

                        // 处理每个键对应的数据
                        for (CompositeKey key : keysToProcess) {
                            List<IndicatorResult> hourData = hourDataMap.get(key);
                            if (hourData != null && hourData.size() > 0) {
                                // 计算小时平均值
                                double avgOccupancy = hourData.stream().mapToDouble(IndicatorResult::getOccupancy).average().orElse(0);
                                double avgHeadway = hourData.stream().mapToDouble(IndicatorResult::getHeadway).average().orElse(0);
                                double avgDelayIndex = hourData.stream().mapToDouble(IndicatorResult::getDelayIndex).average().orElse(0);
                                int totalVehicleCount = hourData.stream().mapToInt(IndicatorResult::getVehicleCount).sum();

                                // 发出指标结果
                                collector.collect(new IndicatorResult(
                                        key.getTimeKey(), "hour", key.getLaneNo(), key.getDirection(), key.getStake(),
                                        avgOccupancy, avgHeadway, avgDelayIndex, totalVehicleCount
                                ));

                                // 清空已处理的数据
                                hourData.clear();
                                hourDataMap.remove(key);
                            }
                        }
                    }
                });

        // 创建天级聚合
        SingleOutputStreamOperator<IndicatorResult> dayIndicatorStream = hourIndicatorStream
                .keyBy(result -> new CompositeKey(
                        getDayFromHour(result.getTimeKey()),
                        result.getLaneNo(),
                        result.getDirection(),
                        result.getStake()
                ))
                .flatMap(new RichFlatMapFunction<IndicatorResult, IndicatorResult>() {
                    private transient Map<CompositeKey, List<IndicatorResult>> dayDataMap;
                    private transient String currentProcessingDay;

                    @Override
                    public void open(Configuration parameters) {
                        dayDataMap = new ConcurrentHashMap<>();
                        currentProcessingDay = "";
                    }

                    @Override
                    public void flatMap(IndicatorResult hourResult, Collector<IndicatorResult> collector) throws Exception {
                        String dayKey = getDayFromHour(hourResult.getTimeKey());

                        // 如果是新的天，处理上一天的数据
                        if (!currentProcessingDay.equals("") && !currentProcessingDay.equals(dayKey)) {
                            processDayData(currentProcessingDay, collector);
                        }

                        currentProcessingDay = dayKey;

                        // 存储小时数据
                        CompositeKey key = new CompositeKey(
                                dayKey,
                                hourResult.getLaneNo(),
                                hourResult.getDirection(),
                                hourResult.getStake()
                        );

                        List<IndicatorResult> dayData = dayDataMap.getOrDefault(key, new ArrayList<>());
                        dayData.add(hourResult);
                        dayDataMap.put(key, dayData);
                    }

                    // 处理指定天的数据并清空
                    private void processDayData(String dayKey, Collector<IndicatorResult> collector) {
                        // 收集所有需要处理的键
                        List<CompositeKey> keysToProcess = new ArrayList<>();
                        for (CompositeKey key : dayDataMap.keySet()) {
                            if (key.getTimeKey().equals(dayKey)) {
                                keysToProcess.add(key);
                            }
                        }

                        // 处理每个键对应的数据
                        for (CompositeKey key : keysToProcess) {
                            List<IndicatorResult> dayData = dayDataMap.get(key);
                            if (dayData != null && dayData.size() > 0) {
                                // 计算天平均值
                                double avgOccupancy = dayData.stream().mapToDouble(IndicatorResult::getOccupancy).average().orElse(0);
                                double avgHeadway = dayData.stream().mapToDouble(IndicatorResult::getHeadway).average().orElse(0);
                                double avgDelayIndex = dayData.stream().mapToDouble(IndicatorResult::getDelayIndex).average().orElse(0);
                                int totalVehicleCount = dayData.stream().mapToInt(IndicatorResult::getVehicleCount).sum();

                                // 发出指标结果
                                collector.collect(new IndicatorResult(
                                        key.getTimeKey(), "day", key.getLaneNo(), key.getDirection(), key.getStake(),
                                        avgOccupancy, avgHeadway, avgDelayIndex, totalVehicleCount
                                ));

                                // 清空已处理的数据
                                dayData.clear();
                                dayDataMap.remove(key);
                            }
                        }
                    }
                });

        // 创建月级聚合
        SingleOutputStreamOperator<IndicatorResult> monthIndicatorStream = dayIndicatorStream
                .keyBy(result -> new CompositeKey(
                        getMonthFromDay(result.getTimeKey()),
                        result.getLaneNo(),
                        result.getDirection(),
                        result.getStake()
                ))
                .flatMap(new RichFlatMapFunction<IndicatorResult, IndicatorResult>() {
                    private transient Map<CompositeKey, List<IndicatorResult>> monthDataMap;
                    private transient String currentProcessingMonth;

                    @Override
                    public void open(Configuration parameters) {
                        monthDataMap = new ConcurrentHashMap<>();
                        currentProcessingMonth = "";
                    }

                    @Override
                    public void flatMap(IndicatorResult dayResult, Collector<IndicatorResult> collector) throws Exception {
                        String monthKey = getMonthFromDay(dayResult.getTimeKey());

                        // 如果是新的月，处理上一月的数据
                        if (!currentProcessingMonth.equals("") && !currentProcessingMonth.equals(monthKey)) {
                            processMonthData(currentProcessingMonth, collector);
                        }

                        currentProcessingMonth = monthKey;

                        // 存储天数据
                        CompositeKey key = new CompositeKey(
                                monthKey,
                                dayResult.getLaneNo(),
                                dayResult.getDirection(),
                                dayResult.getStake()
                        );

                        List<IndicatorResult> monthData = monthDataMap.getOrDefault(key, new ArrayList<>());
                        monthData.add(dayResult);
                        monthDataMap.put(key, monthData);
                    }

                    // 处理指定月的数据并清空
                    private void processMonthData(String monthKey, Collector<IndicatorResult> collector) {
                        // 收集所有需要处理的键
                        List<CompositeKey> keysToProcess = new ArrayList<>();
                        for (CompositeKey key : monthDataMap.keySet()) {
                            if (key.getTimeKey().equals(monthKey)) {
                                keysToProcess.add(key);
                            }
                        }

                        // 处理每个键对应的数据
                        for (CompositeKey key : keysToProcess) {
                            List<IndicatorResult> monthData = monthDataMap.get(key);
                            if (monthData != null && monthData.size() > 0) {
                                // 计算月平均值
                                double avgOccupancy = monthData.stream().mapToDouble(IndicatorResult::getOccupancy).average().orElse(0);
                                double avgHeadway = monthData.stream().mapToDouble(IndicatorResult::getHeadway).average().orElse(0);
                                double avgDelayIndex = monthData.stream().mapToDouble(IndicatorResult::getDelayIndex).average().orElse(0);
                                int totalVehicleCount = monthData.stream().mapToInt(IndicatorResult::getVehicleCount).sum();

                                // 发出指标结果
                                collector.collect(new IndicatorResult(
                                        key.getTimeKey(), "month", key.getLaneNo(), key.getDirection(), key.getStake(),
                                        avgOccupancy, avgHeadway, avgDelayIndex, totalVehicleCount
                                ));

                                // 清空已处理的数据
                                monthData.clear();
                                monthDataMap.remove(key);
                            }
                        }
                    }
                });

//        // 创建Kafka Sink用于输出指标结果
//        KafkaSink<String> minuteSink = createKafkaSink(brokers, minuteTopic);
//        KafkaSink<String> hourSink = createKafkaSink(brokers, hourTopic);
//        KafkaSink<String> daySink = createKafkaSink(brokers, dayTopic);
//        KafkaSink<String> monthSink = createKafkaSink(brokers, monthTopic);
//
//        // 将指标结果转换为字符串并发送到Kafka
//        minuteIndicatorStream
//                .map(IndicatorResult::toString)
//                .sinkTo(minuteSink)
//                .name("Kafka Minute Indicator Sink");
//
//        hourIndicatorStream
//                .map(IndicatorResult::toString)
//                .sinkTo(hourSink)
//                .name("Kafka Hour Indicator Sink");
//
//        dayIndicatorStream
//                .map(IndicatorResult::toString)
//                .sinkTo(daySink)
//                .name("Kafka Day Indicator Sink");
//
//        monthIndicatorStream
//                .map(IndicatorResult::toString)
//                .sinkTo(monthSink)
//                .name("Kafka Month Indicator Sink");

        // 同时打印到控制台用于调试
        minuteIndicatorStream.print();
        hourIndicatorStream.print();
        dayIndicatorStream.print();
        monthIndicatorStream.print();

        env.execute("Traffic Indicator Calculation Job");
    }

    // 创建Kafka Sink
    private static KafkaSink<String> createKafkaSink(String brokers, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }

    // 从分钟键获取小时键
    private static String getHourFromMinute(String minuteKey) {
        return minuteKey.substring(0, 10); // yyyyMMddHH
    }

    // 从小时键获取天键
    private static String getDayFromHour(String hourKey) {
        return hourKey.substring(0, 8); // yyyyMMdd
    }

    // 从天键获取月键
    private static String getMonthFromDay(String dayKey) {
        return dayKey.substring(0, 6); // yyyyMM
    }

    // 计算时间占有率
    private static double calculateOccupancy(List<Pair<Long, Long>> timePairs) {
        if (timePairs == null || timePairs.isEmpty()) {
            return 0.0;
        }

        long totalDuration = 0;
        for (Pair<Long, Long> pair : timePairs) {
            totalDuration += (pair.getValue() - pair.getKey());
        }
        return totalDuration / (60 * 1000.0); // 转换为分钟
    }

    // 计算车头时距
    private static double calculateHeadway(List<Pair<Long, Long>> timePairs) {
        if (timePairs == null || timePairs.size() < 2) {
            return 0.0;
        }

        // 按进入时间排序
        timePairs.sort(Comparator.comparing(Pair::getKey));

        long totalHeadway = 0;
        int count = 0;

        for (int i = 1; i < timePairs.size(); i++) {
            long headway = timePairs.get(i).getKey() - timePairs.get(i - 1).getKey();
            totalHeadway += headway;
            count++;
        }

        return count > 0 ? totalHeadway / (double) count : 0;
    }

    // 计算车辆延时指数
    private static double calculateDelayIndex(List<Pair<Long, Long>> timePairs) {
        if (timePairs == null || timePairs.isEmpty()) {
            return 0.0;
        }

        long totalActualTime = 0;
        double totalFreeFlowTime = 0;

        for (Pair<Long, Long> pair : timePairs) {
            totalActualTime += (pair.getValue() - pair.getKey());
            // 自由流通过时间 = 距离 / 速度
            totalFreeFlowTime += (24.0 / FREEFLOW_SPEED) * 1000; // 转换为毫秒
        }

        return totalActualTime / totalFreeFlowTime;
    }

    // 时间转换方法
    public static long convertToTimestampMillis(String dateTimeStr) {
        // 尝试多种日期格式
        DateTimeFormatter[] formatters = {
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS")
        };

        for (DateTimeFormatter formatter : formatters) {
            try {
                LocalDateTime dateTime = LocalDateTime.parse(dateTimeStr, formatter);
                return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (DateTimeParseException e) {
                // 尝试下一个格式
            }
        }

        throw new IllegalArgumentException("无法解析时间字符串: " + dateTimeStr);
    }

    // 获取分钟字符串
    private static String getMinuteFromTimestamp(long timestamp) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        return dateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
    }

    // 判断是否接近整千桩号
    public static Pair<Boolean, Integer> isNearThousand(double mileage) {
        int roundedThousand = (int) (Math.round(mileage / 1000.0) * 1000);
        double diff = Math.abs(mileage - roundedThousand);
        return new Pair<>(diff <= STAKE_RANGE, roundedThousand);
    }

    // 简单的Pair实现
    public static class Pair<K, V> {
        private K key;
        private V value;

        public Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() { return key; }
        public V getValue() { return value; }

        public void setKey(K key) { this.key = key; }
        public void setValue(V value) { this.value = value; }
    }
}