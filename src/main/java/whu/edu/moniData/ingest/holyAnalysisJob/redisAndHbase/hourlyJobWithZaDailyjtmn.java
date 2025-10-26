package whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import whu.edu.ljj.flink.xiaohanying.Utils.PathPoint;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static whu.edu.ljj.flink.xiaohanying.Utils.convertToTimestampMillis;

public class hourlyJobWithZaDailyjtmn {

    // 路段定义
    private static final List<RoadSection> ROAD_SECTIONS = Arrays.asList(
            new RoadSection("鄂北-大新段", 1016020, 1030448),
            new RoadSection("大新-大悟段", 1030448, 1043400),
            new RoadSection("大悟-阳平段", 1043400, 1058300),
            new RoadSection("阳平-大悟南枢纽段", 1058300, 1062700),
            new RoadSection("大悟南枢纽-小河段", 1062700, 1075200),
            new RoadSection("小河-孝昌段", 1075200, 1092242),
            new RoadSection("孝昌-桃花驿站段", 1092242, 1110002),
            new RoadSection("桃花驿-孝南枢纽段", 1110002, 1115583),
            new RoadSection("孝南枢纽-孝感东段", 1115583, 1122200),
            new RoadSection("孝感东-府河段", 1122200, 1129200),
            new RoadSection("府河-灯塔枢纽段", 1129200, 1140371),
            new RoadSection("灯塔枢纽-东西湖枢纽段", 1140371, 1148571),
            new RoadSection("东西湖枢纽-武汉北段", 1148571, 1153992),
            new RoadSection("武汉北-蔡甸枢纽段", 1153992, 1163000),
            new RoadSection("蔡甸枢纽-天鹅湖段", 1163000, 1168100),
            new RoadSection("天鹅湖-武汉西枢纽段", 1168100, 1173535)
    );

    // 判断客车类型的方法
    private static boolean isBus(int vt) {
        return vt == 1 || vt == 3 || vt == 7 || vt == 15;
    }

    // 判断货车类型的方法
    private static boolean isTruck(int vt) {
        return vt == 2 || vt == 10 || vt == 8 || vt == 11 || vt == 170 || vt == 171 || vt == 172 ||
                vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177;
    }

    // 匝道车辆类型判断方法
    private static int getVehicleClass(int originalType) {
        if ((originalType >= 1 && originalType <= 4) || originalType == 7 || (originalType >= 12 && originalType <= 16)) {
            return 0; // 客车
        }
        if (originalType == 8 || originalType == 10 || originalType == 11 ||
                (originalType >= 170 && originalType <= 177)) {
            return 1; // 货车
        }
        return -1;
    }

    // 根据桩号获取路段起始桩号
    private static String getStakeMarkByMileage(double mileage) {
        int mileageInt = (int) mileage;
        for (RoadSection section : ROAD_SECTIONS) {
            if (mileageInt >= section.startMileage && mileageInt < section.endMileage) {
                // 将起始桩号转换为桩号标记，例如1016020 -> K1016
                int stakeKm = section.startMileage / 1000;
                return "K" + stakeKm;
            }
        }
        return "未知桩号";
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // ==================== 主路数据处理 ====================
        // Kafka配置 - 主路数据
        String brokers = "10.48.53.82:9092";
        String groupId = "hourly-traffic-group";
        List<String> mainRoadTopics = Arrays.asList(
                "jtkj.jga.path"
        );

        // 创建Kafka源 - 主路数据
        KafkaSource<String> mainRoadKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(mainRoadTopics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 主路数据流
        DataStream<String> mainRoadSourceStream = env.fromSource(
                mainRoadKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Main Road Kafka Source"
        );

        // 解析JSON为PathPoint对象 - 主路数据
        SingleOutputStreamOperator<PathPoint> mainRoadPathPointStream = mainRoadSourceStream
                .flatMap(new FlatMapFunction<String, PathPoint>() {
                    @Override
                    public void flatMap(String value, Collector<PathPoint> out) {
                        try {
                            JSONObject json = JSON.parseObject(value);
                            String timestamp = json.getString("timeStamp");
                            JSONArray pathList = json.getJSONArray("pathList");
                            System.out.println("hisjson:");
                            System.out.println(json);
                            for (int i = 0; i < pathList.size(); i++) {
                                PathPoint point = pathList.getObject(i, PathPoint.class);
                                point.setTimeStamp(timestamp);
                                out.collect(point);
                            }
                            ;

                            System.out.println("东八区时间-数据内时间："+(System.currentTimeMillis()-convertToTimestampMillis(timestamp)));
                        } catch (Exception e) {
                            System.err.println("Error parsing JSON: " + e.getMessage());
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PathPoint>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) ->
                                        convertToTimestampMillis(event.getTimeStamp()))
                )
                .name("MainRoadPathPointStream");

        // ==================== 匝道数据处理 ====================
        // Kafka配置 - 匝道数据
        String rampGroupId = "ramp-traffic-group1";
        List<String> rampTopics = Arrays.asList("MergedPathData");

        // 创建Kafka源 - 匝道数据
        KafkaSource<String> rampKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(rampTopics)
                .setGroupId(rampGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 匝道数据流
        DataStream<String> rampSourceStream = env.fromSource(
                rampKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Ramp Kafka Source"
        );

        // 解析JSON为PathPoint对象 - 匝道数据
        SingleOutputStreamOperator<PathPoint> rampPathPointStream = rampSourceStream
                .flatMap(new FlatMapFunction<String, PathPoint>() {
                    @Override
                    public void flatMap(String value, Collector<PathPoint> out) {
                        try {
                            JSONObject json = JSON.parseObject(value);
                            String timestamp = json.getString("timeStamp");
                            JSONArray pathList = json.getJSONArray("pathList");

                            for (int i = 0; i < pathList.size(); i++) {
                                PathPoint point = pathList.getObject(i, PathPoint.class);
                                point.setTimeStamp(timestamp);
                                out.collect(point);
                            }
                        } catch (Exception e) {
                            System.err.println("Error parsing JSON: "+ e.getMessage());
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PathPoint>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) ->
                                        convertToTimestampMillis(event.getTimeStamp()))
                )
                .name("RampPathPointStream");

        // ==================== 主路交通量统计（按小时和方向）====================
        DataStream<Tuple3<String, Integer, Integer>> totalTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple3<String, Long, Integer>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple3<String, Long, Integer>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = convertToTimestampMillis(point.getTimeStamp());
                            String hourKey = new SimpleDateFormat("yyyyMMddHH").format(eventTime);
                            out.collect(new Tuple3<>(hourKey, point.getId(), point.getDirection()));
                        }
                    }
                })
                .keyBy(t -> t.f0)  // 按小时分组
                .window(TumblingEventTimeWindows.of(Time.hours(1))) // 1小时滚动窗口
                .aggregate(new TotalTrafficAggregator())
                .name("TotalTrafficStream");

        // 直接输出总交通量
        totalTrafficStream.print("总交通量统计");

        // ==================== 主路详细交通量统计（按小时、路段、方向和类型）====================
        DataStream<Tuple6<String, String, Integer, Integer, Integer, Integer>> detailedTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple6<String, String, Integer, Long, Integer, Integer>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple6<String, String, Integer, Long, Integer, Integer>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = convertToTimestampMillis(point.getTimeStamp());
                            String hourKey = new SimpleDateFormat("yyyyMMddHH").format(eventTime);

                            // 根据桩号获取路段起始桩号
                            String stakeMark = getStakeMarkByMileage(point.getMileage());

                            // 判断车辆类型
                            int vehicleType = point.getVehicleType();
                            int isBus = isBus(vehicleType) ? 1 : 0;
                            int isTruck = isTruck(vehicleType) ? 1 : 0;

                            out.collect(new Tuple6<>(hourKey, stakeMark, point.getDirection(), point.getId(), isBus, isTruck));
                        }
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1 + "_" + t.f2)  // 按小时+桩号+方向分组
                .window(TumblingEventTimeWindows.of(Time.hours(1))) // 1小时滚动窗口
                .aggregate(new DetailedTrafficAggregator())
                .name("DetailedTrafficStream");

        // 直接输出详细交通量
        detailedTrafficStream.print("详细交通量统计");

        // ==================== 匝道交通量统计 ====================
        DataStream<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> rampTrafficStream = rampPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple7<String, String, Long, Integer, Double, Integer, Integer>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple7<String, String, Long, Integer, Double, Integer, Integer>> out) {
                        // 检查是否为匝道数据
                        if (point.getStakeId() != null && point.getStakeId().contains("-")) {
                            String[] parts = point.getStakeId().split("-");
                            if (parts.length >= 2) {
                                // 提取匝道编号 (CK0+199 -> C)
                                String rampCode = parts[1].substring(0, 1);
                                if (rampCode.matches("[A-D]")) { // 只处理A,B,C,D四种匝道
                                    long eventTime = convertToTimestampMillis(point.getTimeStamp());
                                    String hourKey = new SimpleDateFormat("yyyyMMddHH").format(eventTime);

                                    // 判断车辆类型
                                    int vehicleClass = getVehicleClass(point.getOriginalType());
                                    int isBus = (vehicleClass == 0) ? 1 : 0;
                                    int isTruck = (vehicleClass == 1) ? 1 : 0;

                                    // 修复这里：使用new Tuple7<>()而不是Tuple7.of()
                                    out.collect(new Tuple7<>(hourKey, rampCode, point.getId(), isBus, point.getSpeed(), isTruck, 1));
                                }
                            }
                        }
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1)  // 按小时+匝道编号分组
                .window(TumblingEventTimeWindows.of(Time.hours(1))) // 1小时滚动窗口
                .aggregate(new RampTrafficAggregator())
                .name("RampTrafficStream");

        // 直接输出匝道交通量
        rampTrafficStream.print("匝道交通量统计");

        // ==================== 每日去重统计（按两小时去重）====================
        // 每日总交通量统计（按天和方向）
        DataStream<Tuple3<String, Integer, Integer>> dailyTotalTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple4<String, Long, Integer, Long>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple4<String, Long, Integer, Long>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = convertToTimestampMillis(point.getTimeStamp());
                            String dayKey = new SimpleDateFormat("yyyyMMdd").format(eventTime);

                            // 修复这里：使用new Tuple4<>()而不是Tuple4.of()
                            out.collect(new Tuple4<>(dayKey, point.getId(), point.getDirection(), eventTime));
                        }
                    }
                })
                .keyBy(t -> t.f0)  // 按天分组
                .window(TumblingEventTimeWindows.of(Time.days(1))) // 1天滚动窗口
                .aggregate(new DailyTotalTrafficAggregator())
                .name("DailyTotalTrafficStream");

        // 直接输出每日总交通量
        dailyTotalTrafficStream.print("每日总交通量统计");

        // 每日详细交通量统计（按天、路段、方向和类型）
        DataStream<Tuple6<String, String, Integer, Integer, Integer, Integer>> dailyDetailedTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple7<String, String, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple7<String, String, Integer, Long, Integer, Integer, Long>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = convertToTimestampMillis(point.getTimeStamp());
                            String dayKey = new SimpleDateFormat("yyyyMMdd").format(eventTime);

                            // 根据桩号获取路段起始桩号
                            String stakeMark = getStakeMarkByMileage(point.getMileage());

                            // 判断车辆类型
                            int vehicleType = point.getVehicleType();
                            int isBus = isBus(vehicleType) ? 1 : 0;
                            int isTruck = isTruck(vehicleType) ? 1 : 0;

                            // 修复这里：使用new Tuple7<>()而不是Tuple7.of()
                            out.collect(new Tuple7<>(dayKey, stakeMark, point.getDirection(), point.getId(), isBus, isTruck, eventTime));
                        }
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1 + "_" + t.f2)  // 按天+桩号+方向分组
                .window(TumblingEventTimeWindows.of(Time.days(1))) // 1天滚动窗口
                .aggregate(new DailyDetailedTrafficAggregator())
                .name("DailyDetailedTrafficStream");

        // 直接输出每日详细交通量
        dailyDetailedTrafficStream.print("每日详细交通量统计");

        env.execute("Combined Hourly and Daily Traffic Analysis");
    }

    // ==================== 路段定义类 ====================
    private static class RoadSection {
        String sectionName;
        int startMileage;
        int endMileage;

        public RoadSection(String sectionName, int startMileage, int endMileage) {
            this.sectionName = sectionName;
            this.startMileage = startMileage;
            this.endMileage = endMileage;
        }
    }

    // ==================== 总交通量聚合器和累加器 ====================
    private static class TotalTrafficAggregator implements AggregateFunction<
            Tuple3<String, Long, Integer>,
            TotalTrafficAccumulator,
            Tuple3<String, Integer, Integer>> {

        @Override
        public TotalTrafficAccumulator createAccumulator() {
            return new TotalTrafficAccumulator();
        }

        @Override
        public TotalTrafficAccumulator add(Tuple3<String, Long, Integer> value, TotalTrafficAccumulator acc) {
            if (acc.hourKey == null) {
                acc.hourKey = value.f0;
            }
            acc.addVehicle(value.f1, value.f2);
            return acc;
        }

        @Override
        public Tuple3<String, Integer, Integer> getResult(TotalTrafficAccumulator acc) {
            return Tuple3.of(acc.hourKey, acc.upCount.get(), acc.downCount.get());
        }

        @Override
        public TotalTrafficAccumulator merge(TotalTrafficAccumulator a, TotalTrafficAccumulator b) {
            a.merge(b);
            return a;
        }
    }

    private static class TotalTrafficAccumulator {
        public String hourKey;
        public final Set<Long> vehicleIds = new HashSet<>();
        public final AtomicInteger upCount = new AtomicInteger(0);
        public final AtomicInteger downCount = new AtomicInteger(0);

        public void addVehicle(long vehicleId, int direction) {
            if (!vehicleIds.contains(vehicleId)) {
                vehicleIds.add(vehicleId);
                if (direction == 1) {
                    upCount.incrementAndGet();
                } else if (direction == 2) {
                    downCount.incrementAndGet();
                }
            }
        }

        public void merge(TotalTrafficAccumulator other) {
            for (Long id : other.vehicleIds) {
                if (!vehicleIds.contains(id)) {
                    vehicleIds.add(id);
                    if (other.upCount.get() > 0) upCount.incrementAndGet();
                    if (other.downCount.get() > 0) downCount.incrementAndGet();
                }
            }
        }
    }

    // ==================== 详细交通量聚合器和累加器 ====================
    private static class DetailedTrafficAggregator implements AggregateFunction<
            Tuple6<String, String, Integer, Long, Integer, Integer>,
            DetailedTrafficAccumulator,
            Tuple6<String, String, Integer, Integer, Integer, Integer>> {

        @Override
        public DetailedTrafficAccumulator createAccumulator() {
            return new DetailedTrafficAccumulator();
        }

        @Override
        public DetailedTrafficAccumulator add(Tuple6<String, String, Integer, Long, Integer, Integer> value, DetailedTrafficAccumulator acc) {
            if (acc.hourKey == null) {
                acc.hourKey = value.f0;
                acc.stakeMark = value.f1;
                acc.direction = value.f2;
            }
            acc.addVehicle(value.f3, value.f4, value.f5);
            return acc;
        }

        @Override
        public Tuple6<String, String, Integer, Integer, Integer, Integer> getResult(DetailedTrafficAccumulator acc) {
            return Tuple6.of(acc.hourKey, acc.stakeMark, acc.direction,
                    acc.busCount.get(), acc.truckCount.get(), acc.otherCount.get());
        }

        @Override
        public DetailedTrafficAccumulator merge(DetailedTrafficAccumulator a, DetailedTrafficAccumulator b) {
            a.merge(b);
            return a;
        }
    }

    private static class DetailedTrafficAccumulator {
        public String hourKey;
        public String stakeMark; // 桩号标记，如K1016
        public int direction;
        public final Set<Long> vehicleIds = new HashSet<>();
        public final AtomicInteger busCount = new AtomicInteger(0);
        public final AtomicInteger truckCount = new AtomicInteger(0);
        public final AtomicInteger otherCount = new AtomicInteger(0);

        public void addVehicle(long vehicleId, int isBus, int isTruck) {
            if (!vehicleIds.contains(vehicleId)) {
                vehicleIds.add(vehicleId);
                if (isBus == 1) {
                    busCount.incrementAndGet();
                } else if (isTruck == 1) {
                    truckCount.incrementAndGet();
                } else {
                    otherCount.incrementAndGet();
                }
            }
        }

        public void merge(DetailedTrafficAccumulator other) {
            for (Long id : other.vehicleIds) {
                if (!vehicleIds.contains(id)) {
                    vehicleIds.add(id);
                    busCount.addAndGet(other.busCount.get());
                    truckCount.addAndGet(other.truckCount.get());
                    otherCount.addAndGet(other.otherCount.get());
                }
            }
        }
    }

    // ==================== 匝道交通量聚合器和累加器 ====================
    private static class RampTrafficAggregator implements AggregateFunction<
            Tuple7<String, String, Long, Integer, Double, Integer, Integer>,
            RampTrafficAccumulator,
            Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> {

        @Override
        public RampTrafficAccumulator createAccumulator() {
            return new RampTrafficAccumulator();
        }

        @Override
        public RampTrafficAccumulator add(Tuple7<String, String, Long, Integer, Double, Integer, Integer> value, RampTrafficAccumulator acc) {
            if (acc.hourKey == null) {
                acc.hourKey = value.f0;
                acc.rampCode = value.f1;
            }
            acc.addVehicle(value.f2, value.f3, value.f4, value.f5, value.f6);
            return acc;
        }

        @Override
        public Tuple7<String, String, Integer, Integer, Integer, Double, Integer> getResult(RampTrafficAccumulator acc) {
            double avgSpeed = acc.vehicleCount.get() > 0 ? acc.totalSpeed.get() / acc.vehicleCount.get() : 0.0;
            return Tuple7.of(acc.hourKey, acc.rampCode, acc.vehicleCount.get(),
                    acc.busCount.get(), acc.truckCount.get(), avgSpeed, acc.totalCount.get());
        }

        @Override
        public RampTrafficAccumulator merge(RampTrafficAccumulator a, RampTrafficAccumulator b) {
            a.merge(b);
            return a;
        }
    }

    private static class RampTrafficAccumulator {
        public String hourKey;
        public String rampCode;
        public final Set<Long> vehicleIds = new HashSet<>();
        public final AtomicInteger busCount = new AtomicInteger(0);
        public final AtomicInteger truckCount = new AtomicInteger(0);
        public final AtomicInteger vehicleCount = new AtomicInteger(0);
        public final AtomicInteger totalCount = new AtomicInteger(0);
        public final AtomicDouble totalSpeed = new AtomicDouble(0.0);

        public void addVehicle(long vehicleId, int isBus, double speed, int isTruck, int count) {
            totalCount.addAndGet(count);
            totalSpeed.addAndGet(speed);

            if (!vehicleIds.contains(vehicleId)) {
                vehicleIds.add(vehicleId);
                vehicleCount.incrementAndGet();
                if (isBus == 1) {
                    busCount.incrementAndGet();
                } else if (isTruck == 1) {
                    truckCount.incrementAndGet();
                }
            }
        }

        public void merge(RampTrafficAccumulator other) {
            for (Long id : other.vehicleIds) {
                if (!vehicleIds.contains(id)) {
                    vehicleIds.add(id);
                    vehicleCount.addAndGet(1);
                    busCount.addAndGet(other.busCount.get());
                    truckCount.addAndGet(other.truckCount.get());
                }
            }
            totalCount.addAndGet(other.totalCount.get());
            totalSpeed.addAndGet(other.totalSpeed.get());
        }
    }

    // ==================== 每日总交通量聚合器（按两小时去重）====================
    private static class DailyTotalTrafficAggregator implements AggregateFunction<
            Tuple4<String, Long, Integer, Long>,
            DailyTotalTrafficAccumulator,
            Tuple3<String, Integer, Integer>> {

        @Override
        public DailyTotalTrafficAccumulator createAccumulator() {
            return new DailyTotalTrafficAccumulator();
        }

        @Override
        public DailyTotalTrafficAccumulator add(Tuple4<String, Long, Integer, Long> value, DailyTotalTrafficAccumulator acc) {
            if (acc.dayKey == null) {
                acc.dayKey = value.f0;
            }
            acc.addVehicle(value.f1, value.f2, value.f3);
            return acc;
        }

        @Override
        public Tuple3<String, Integer, Integer> getResult(DailyTotalTrafficAccumulator acc) {
            return Tuple3.of(acc.dayKey, acc.upCount, acc.downCount);
        }

        @Override
        public DailyTotalTrafficAccumulator merge(DailyTotalTrafficAccumulator a, DailyTotalTrafficAccumulator b) {
            // 合并两个累加器
            for (Map.Entry<String, Set<Long>> entry : b.twoHourWindows.entrySet()) {
                String twoHourKey = entry.getKey();
                Set<Long> vehicleSet = a.twoHourWindows.computeIfAbsent(twoHourKey, k -> new HashSet<>());
                for (Long vehicleId : entry.getValue()) {
                    if (!vehicleSet.contains(vehicleId)) {
                        vehicleSet.add(vehicleId);
                        // 注意：这里我们无法知道方向，所以不能重新计数方向
                    }
                }
            }
            // 重新计算计数（因为方向信息丢失）
            a.recalculateCounts();
            return a;
        }
    }

    private static class DailyTotalTrafficAccumulator {
        public String dayKey;
        // 两小时窗口 -> 车辆ID集合
        public Map<String, Set<Long>> twoHourWindows = new HashMap<>();
        public int upCount = 0;
        public int downCount = 0;
        // 临时存储方向信息
        private Map<Long, Integer> vehicleDirections = new HashMap<>();

        public void addVehicle(long vehicleId, int direction, long timestamp) {
            // 存储车辆方向
            vehicleDirections.put(vehicleId, direction);

            // 将时间戳转换为两小时窗口的起始时间字符串
            String twoHourKey = getTwoHourWindowKey(timestamp);
            Set<Long> vehicleSet = twoHourWindows.computeIfAbsent(twoHourKey, k -> new HashSet<>());

            if (!vehicleSet.contains(vehicleId)) {
                vehicleSet.add(vehicleId);
                if (direction == 1) {
                    upCount++;
                } else if (direction == 2) {
                    downCount++;
                }
            }
        }

        private String getTwoHourWindowKey(long timestamp) {
            // 将时间戳转换为两小时窗口的起始时间
            Date date = new Date(timestamp);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            hour = (hour / 2) * 2; // 取整到两小时
            calendar.set(Calendar.HOUR_OF_DAY, hour);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            return new SimpleDateFormat("yyyyMMddHH").format(calendar.getTime());
        }

        public void recalculateCounts() {
            upCount = 0;
            downCount = 0;

            for (Set<Long> vehicleSet : twoHourWindows.values()) {
                for (Long vehicleId : vehicleSet) {
                    Integer direction = vehicleDirections.get(vehicleId);
                    if (direction != null) {
                        if (direction == 1) {
                            upCount++;
                        } else if (direction == 2) {
                            downCount++;
                        }
                    }
                }
            }
        }
    }

    // ==================== 每日详细交通量聚合器（按两小时去重）====================
    private static class DailyDetailedTrafficAggregator implements AggregateFunction<
            Tuple7<String, String, Integer, Long, Integer, Integer, Long>,
            DailyDetailedTrafficAccumulator,
            Tuple6<String, String, Integer, Integer, Integer, Integer>> {

        @Override
        public DailyDetailedTrafficAccumulator createAccumulator() {
            return new DailyDetailedTrafficAccumulator();
        }

        @Override
        public DailyDetailedTrafficAccumulator add(Tuple7<String, String, Integer, Long, Integer, Integer, Long> value, DailyDetailedTrafficAccumulator acc) {
            if (acc.dayKey == null) {
                acc.dayKey = value.f0;
                acc.stakeMark = value.f1;
                acc.direction = value.f2;
            }
            acc.addVehicle(value.f3, value.f4, value.f5, value.f6);
            return acc;
        }

        @Override
        public Tuple6<String, String, Integer, Integer, Integer, Integer> getResult(DailyDetailedTrafficAccumulator acc) {
            return Tuple6.of(acc.dayKey, acc.stakeMark, acc.direction,
                    acc.busCount, acc.truckCount, acc.otherCount);
        }

        @Override
        public DailyDetailedTrafficAccumulator merge(DailyDetailedTrafficAccumulator a, DailyDetailedTrafficAccumulator b) {
            // 合并两个累加器
            for (Map.Entry<String, Set<Long>> entry : b.twoHourWindows.entrySet()) {
                String twoHourKey = entry.getKey();
                Set<Long> vehicleSet = a.twoHourWindows.computeIfAbsent(twoHourKey, k -> new HashSet<>());
                vehicleSet.addAll(entry.getValue());
            }
            // 重新计算计数
            a.recalculateCounts();
            return a;
        }
    }

    private static class DailyDetailedTrafficAccumulator {
        public String dayKey;
        public String stakeMark; // 桩号标记，如K1016
        public int direction;
        // 两小时窗口 -> 车辆ID集合
        public Map<String, Set<Long>> twoHourWindows = new HashMap<>();
        public int busCount = 0;
        public int truckCount = 0;
        public int otherCount = 0;
        // 临时存储车辆类型信息
        private Map<Long, Integer> vehicleBusMap = new HashMap<>();
        private Map<Long, Integer> vehicleTruckMap = new HashMap<>();

        public void addVehicle(long vehicleId, int isBus, int isTruck, long timestamp) {
            // 存储车辆类型信息
            vehicleBusMap.put(vehicleId, isBus);
            vehicleTruckMap.put(vehicleId, isTruck);

            // 将时间戳转换为两小时窗口的起始时间字符串
            String twoHourKey = getTwoHourWindowKey(timestamp);
            Set<Long> vehicleSet = twoHourWindows.computeIfAbsent(twoHourKey, k -> new HashSet<>());

            if (!vehicleSet.contains(vehicleId)) {
                vehicleSet.add(vehicleId);
                if (isBus == 1) {
                    busCount++;
                } else if (isTruck == 1) {
                    truckCount++;
                } else {
                    otherCount++;
                }
            }
        }

        private String getTwoHourWindowKey(long timestamp) {
            // 将时间戳转换为两小时窗口的起始时间
            Date date = new Date(timestamp);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int hour = calendar.get(Calendar.HOUR_OF_DAY);
            hour = (hour / 2) * 2; // 取整到两小时
            calendar.set(Calendar.HOUR_OF_DAY, hour);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            return new SimpleDateFormat("yyyyMMddHH").format(calendar.getTime());
        }

        public void recalculateCounts() {
            busCount = 0;
            truckCount = 0;
            otherCount = 0;

            for (Set<Long> vehicleSet : twoHourWindows.values()) {
                for (Long vehicleId : vehicleSet) {
                    Integer isBus = vehicleBusMap.get(vehicleId);
                    Integer isTruck = vehicleTruckMap.get(vehicleId);

                    if (isBus != null && isBus == 1) {
                        busCount++;
                    } else if (isTruck != null && isTruck == 1) {
                        truckCount++;
                    } else {
                        otherCount++;
                    }
                }
            }
        }
    }

    // 简单的原子Double类
    private static class AtomicDouble {
        private double value = 0.0;

        public AtomicDouble(double v) {
            value=v;
        }

        public void addAndGet(double delta) {
            synchronized (this) {
                value += delta;
            }
        }

        public double get() {
            synchronized (this) {
                return value;
            }
        }
    }
}