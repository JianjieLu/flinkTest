package whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.agoVersions;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
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

public class buqua1n3 {

    // 自由流速度 (120km/h -> m/s)
    private static final double FREEFLOW_SPEED = 120.0 * 1000 / 3600;
    private static final int STAKE_RANGE = 12;
    private static final double DETECTION_LENGTH = 24.0; // 检测断面长度(米)

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
        private String calculationLog; // 添加计算过程日志

        public IndicatorResult(String timeKey, String timeType, int laneNo, int direction, int stake,
                               double occupancy, double headway, double delayIndex, int vehicleCount, String calculationLog) {
            this.timeKey = timeKey;
            this.timeType = timeType;
            this.laneNo = laneNo;
            this.direction = direction;
            this.stake = stake;
            this.occupancy = occupancy;
            this.headway = headway;
            this.delayIndex = delayIndex;
            this.vehicleCount = vehicleCount;
            this.calculationLog = calculationLog;
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
        public String getCalculationLog() { return calculationLog; }

        @Override
        public String toString() {
            return String.format("%s,%s,%d,%d,%d,%.4f,%.4f,%.4f,%d | %s",
                    timeKey, timeType, laneNo, direction, stake,
                    occupancy, headway, delayIndex, vehicleCount, calculationLog);
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
                    // 存储每个时间段的最小和最大时间戳
                    private transient Map<CompositeKey, Pair<Long, Long>> timeRangeMap;

                    @Override
                    public void open(Configuration parameters) {
                        timeDataMap = new ConcurrentHashMap<>();
                        timeRangeMap = new ConcurrentHashMap<>();
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

                                // 更新时间段范围
                                Pair<Long, Long> timeRange = timeRangeMap.get(key);
                                if (timeRange == null) {
                                    timeRange = new Pair<>(Long.MAX_VALUE, Long.MIN_VALUE);
                                    timeRangeMap.put(key, timeRange);
                                }

                                // 更新最小和最大时间戳
                                if (timestamp < timeRange.getKey()) {
                                    timeRange.setKey(timestamp);
                                }
                                if (timestamp > timeRange.getValue()) {
                                    timeRange.setValue(timestamp);
                                }

                                // 使用车辆速度和检测区长度计算占用时间
                                double speed = point.getSpeed(); // 获取车辆速度(m/s)
                                if (speed <= 0) {
                                    // 如果速度无效，使用自由流速度
                                    speed = FREEFLOW_SPEED;
                                }

                                // 计算占用时间 = 检测区长度 / 速度 (转换为毫秒)
                                long occupancyTime = (long) ((DETECTION_LENGTH / speed) * 1000);
                                long enterTime = timestamp;
                                long exitTime = enterTime + occupancyTime;

                                timePairs.add(new Pair<>(enterTime, exitTime));
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
                            if (timePairs != null && !timePairs.isEmpty()) {
                                Pair<Long, Long> timeRange = timeRangeMap.get(key);

                                // 计算指标
                                IndicatorCalculationResult result = calculateIndicators(timePairs, timeRange);

                                // 创建计算过程日志
                                String log = String.format(
                                        "Occupancy: %dms/%dms=%.4f | Headway: %dms/%d=%.4f | Delay: %dms/(%dms*%d)=%.4f | Vehicles: %d",
                                        result.totalOccupancy, result.timeRangeMs, result.occupancy,
                                        result.timeRangeMs, result.vehicleCount, result.headway,
                                        result.totalActualTime, result.freeFlowTimePerVehicle, result.vehicleCount, result.delayIndex,
                                        timePairs.size()
                                );

                                // 发出指标结果
                                collector.collect(new IndicatorResult(
                                        key.getTimeKey(), "minute", key.getLaneNo(), key.getDirection(), key.getStake(),
                                        result.occupancy, result.headway, result.delayIndex, timePairs.size(), log
                                ));

                                // 清空已处理的数据
                                timeDataMap.remove(key);
                                timeRangeMap.remove(key);
                            }
                        }
                    }

                    // 指标计算结果容器
                    class IndicatorCalculationResult {
                        double occupancy;
                        double headway;
                        double delayIndex;
                        long totalOccupancy;
                        long totalActualTime;
                        long freeFlowTimePerVehicle; // 每辆车的自由流通过时间
                        long timeRangeMs;   // 时间段持续时间
                        int vehicleCount;    // 车辆数量
                    }

                    // 计算所有指标
                    private IndicatorCalculationResult calculateIndicators(List<Pair<Long, Long>> timePairs, Pair<Long, Long> timeRange) {
                        IndicatorCalculationResult result = new IndicatorCalculationResult();
                        result.vehicleCount = timePairs.size();

                        // 计算时间段持续时间
                        long minTime = timeRange.getKey();
                        long maxTime = timeRange.getValue();
                        result.timeRangeMs = maxTime - minTime;

                        // 1. 计算时间占有率
                        result.totalOccupancy = 0;
                        for (Pair<Long, Long> pair : timePairs) {
                            result.totalOccupancy += (pair.getValue() - pair.getKey());
                        }
                        // 占有率 = 总占用时间 / 时间段持续时间
                        result.occupancy = result.totalOccupancy / (double) result.timeRangeMs;

                        // 2. 计算车头时距
                        if (result.vehicleCount > 0) {
                            // 平均车头时距 = 时间段持续时间 / 车辆数量
                            result.headway = result.timeRangeMs / (double) result.vehicleCount;
                        } else {
                            result.headway = 0;
                        }

                        // 3. 计算车辆延时指数（修正后的正确公式）
                        result.totalActualTime = 0;
                        for (Pair<Long, Long> pair : timePairs) {
                            result.totalActualTime += (pair.getValue() - pair.getKey());
                        }

                        // 计算每辆车的自由流通过时间（毫秒）
                        result.freeFlowTimePerVehicle = (long) ((DETECTION_LENGTH / FREEFLOW_SPEED) * 1000);

                        // 正确的延时指数计算
                        if (result.vehicleCount > 0) {
                            result.delayIndex = result.totalActualTime /
                                    (double) (result.freeFlowTimePerVehicle * result.vehicleCount);
                        } else {
                            result.delayIndex = 0;
                        }

                        return result;
                    }

                    @Override
                    public void close() {
                        // 处理剩余数据（最后一分钟的数据）
                        if (!currentProcessingMinute.equals("")) {
                            processMinuteData(currentProcessingMinute, new Collector<IndicatorResult>() {
                                @Override
                                public void collect(IndicatorResult record) {
                                    // 输出到日志或存储
                                    System.out.println("Final minute data: " + record);
                                }

                                @Override
                                public void close() {}
                            });
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
                        // 收集所有需要处理的极
                        List<CompositeKey> keysToProcess = new ArrayList<>();
                        for (CompositeKey key : hourDataMap.keySet()) {
                            if (key.getTimeKey().equals(hourKey)) {
                                keysToProcess.add(key);
                            }
                        }

                        // 处理每个键对应的数据
                        for (CompositeKey key : keysToProcess) {
                            List<IndicatorResult> hourData = hourDataMap.get(key);
                            if (hourData != null && !hourData.isEmpty()) {
                                // 计算小时平均值
                                double avgOccupancy = hourData.stream().mapToDouble(IndicatorResult::getOccupancy).average().orElse(0);
                                double avgHeadway = hourData.stream().mapToDouble(IndicatorResult::getHeadway).average().orElse(0);
                                double avgDelayIndex = hourData.stream().mapToDouble(IndicatorResult::getDelayIndex).average().orElse(0);
                                int totalVehicleCount = hourData.stream().mapToInt(IndicatorResult::getVehicleCount).sum();

                                // 创建聚合日志
                                StringBuilder log = new StringBuilder("Hourly Aggregation: ");
                                log.append("AvgOccupancy=").append(avgOccupancy)
                                        .append(", AvgHeadway=").append(avgHeadway)
                                        .append(", AvgDelayIndex=").append(avgDelayIndex)
                                        .append(", TotalVehicles=").append(totalVehicleCount)
                                        .append(" | From ").append(hourData.size()).append(" minute records");

                                // 发出指标结果
                                collector.collect(new IndicatorResult(
                                        key.getTimeKey(), "hour", key.getLaneNo(), key.getDirection(), key.getStake(),
                                        avgOccupancy, avgHeadway, avgDelayIndex, totalVehicleCount, log.toString()
                                ));

                                // 清空已处理的数据
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
                            if (dayData != null && !dayData.isEmpty()) {
                                // 计算天平均值
                                double avgOccupancy = dayData.stream().mapToDouble(IndicatorResult::getOccupancy).average().orElse(0);
                                double avgHeadway = dayData.stream().mapToDouble(IndicatorResult::getHeadway).average().orElse(0);
                                double avgDelayIndex = dayData.stream().mapToDouble(IndicatorResult::getDelayIndex).average().orElse(0);
                                int totalVehicleCount = dayData.stream().mapToInt(IndicatorResult::getVehicleCount).sum();

                                // 创建聚合日志
                                StringBuilder log = new StringBuilder("Daily Aggregation: ");
                                log.append("AvgOccupancy=").append(avgOccupancy)
                                        .append(", AvgHeadway=").append(avgHeadway)
                                        .append(", AvgDelayIndex=").append(avgDelayIndex)
                                        .append(", TotalVehicles=").append(totalVehicleCount)
                                        .append(" | From ").append(dayData.size()).append(" hour records");

                                // 发出指标结果
                                collector.collect(new IndicatorResult(
                                        key.getTimeKey(), "day", key.getLaneNo(), key.getDirection(), key.getStake(),
                                        avgOccupancy, avgHeadway, avgDelayIndex, totalVehicleCount, log.toString()
                                ));

                                // 清空已处理的数据
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
                            if (monthData != null && !monthData.isEmpty()) {
                                // 计算月平均值
                                double avgOccupancy = monthData.stream().mapToDouble(IndicatorResult::getOccupancy).average().orElse(0);
                                double avgHeadway = monthData.stream().mapToDouble(IndicatorResult::getHeadway).average().orElse(0);
                                double avgDelayIndex = monthData.stream().mapToDouble(IndicatorResult::getDelayIndex).average().orElse(0);
                                int totalVehicleCount = monthData.stream().mapToInt(IndicatorResult::getVehicleCount).sum();

                                // 创建聚合日志
                                StringBuilder log = new StringBuilder("Monthly Aggregation: ");
                                log.append("AvgOccupancy=").append(avgOccupancy)
                                        .append(", AvgHeadway=").append(avgHeadway)
                                        .append(", AvgDelayIndex=").append(avgDelayIndex)
                                        .append(", TotalVehicles=").append(totalVehicleCount)
                                        .append(" | From ").append(monthData.size()).append(" day records");

                                // 发出指标结果
                                collector.collect(new IndicatorResult(
                                        key.getTimeKey(), "month", key.getLaneNo(), key.getDirection(), key.getStake(),
                                        avgOccupancy, avgHeadway, avgDelayIndex, totalVehicleCount, log.toString()
                                ));

                                // 清空已处理的数据
                                monthDataMap.remove(key);
                            }
                        }
                    }
                });

        // 打印所有级别的指标到控制台
        minuteIndicatorStream.print("Minute Indicators");
        hourIndicatorStream.print("Hour Indicators");
        dayIndicatorStream.print("Day Indicators");
        monthIndicatorStream.print("Month Indicators");

        env.execute("Traffic Indicator Calculation Job");
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