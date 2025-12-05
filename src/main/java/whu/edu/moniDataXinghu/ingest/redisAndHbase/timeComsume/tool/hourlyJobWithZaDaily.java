package whu.edu.moniDataXinghu.ingest.redisAndHbase.timeComsume.tool;

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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import whu.edu.ljj.flink.xiaohanying.Utils;
import whu.edu.ljj.flink.xiaohanying.Utils.PathPoint;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
//flink run -c whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.hourlyJobWithZaDaily /home/ljj/totalInfo/flinkTest-1.0-SNAPSHOT.jar  100.65.38.40:9092 e1_data_XG01

public class hourlyJobWithZaDaily {
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
    private static final ReentrantLock tableLock = new ReentrantLock();

    // 表名常量
    private static final String TABLE_NAME_TOTAL = "traffic_stats";
    private static final String TABLE_NAME_DETAIL = "traffic_stats_by_section";
    private static final String TABLE_NAME_RAMP = "ramp_traffic_stats";
    private static final String TABLE_NAME_DAILY_TOTAL = "daily_traffic_stats";
    private static final String TABLE_NAME_DAILY_DETAIL = "daily_traffic_stats_by_section";
    private static final String COLUMN_FAMILY = "stats";

    // 监控统计
    private static final AtomicLong totalMemoryUsage = new AtomicLong(0);
    private static final AtomicLong totalStorageSize = new AtomicLong(0);
    private static final ConcurrentHashMap<String, TableStats> tableStatistics = new ConcurrentHashMap<>();

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

    // 统计数据结构
    private static class TableStats {
        private final AtomicLong writeCount = new AtomicLong(0);
        private final AtomicLong totalLatency = new AtomicLong(0);
        private final AtomicLong totalStorageSize = new AtomicLong(0);
        private final AtomicLong maxLatency = new AtomicLong(0);
        private final AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);

        public void recordWrite(long latency, long storageSize) {
            writeCount.incrementAndGet();
            totalLatency.addAndGet(latency);
            totalStorageSize.addAndGet(storageSize);
            maxLatency.set(Math.max(maxLatency.get(), latency));
            if (latency < minLatency.get()) {
                minLatency.set(latency);
            }
        }

        public void printStats() {
            long count = writeCount.get();
            if (count > 0) {
                double avgLatency = totalLatency.get() / (double) count;
                double avgStorage = totalStorageSize.get() / (double) count;
                System.out.printf("Table Stats - Writes: %d, Avg Latency: %.2f ms, Min/Max Latency: %d/%d ms, " +
                                "Total Storage: %d bytes, Avg Storage/Write: %.2f bytes%n",
                        count, avgLatency, minLatency.get(), maxLatency.get(),
                        totalStorageSize.get(), avgStorage);
            }
        }
    }

    // 内存监控类
    private static class MemoryMonitor {
        private static final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

        public static void printMemoryUsage(String prefix) {
            MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
            MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();

            System.out.printf("%s - Heap: Used=%dMB, Max=%dMB, Committed=%dMB | " +
                            "Non-Heap: Used=%dMB, Committed=%dMB%n",
                    prefix,
                    heapMemoryUsage.getUsed() / (1024 * 1024),
                    heapMemoryUsage.getMax() / (1024 * 1024),
                    heapMemoryUsage.getCommitted() / (1024 * 1024),
                    nonHeapMemoryUsage.getUsed() / (1024 * 1024),
                    nonHeapMemoryUsage.getCommitted() / (1024 * 1024));
        }

        public static long getTotalMemoryUsage() {
            MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
            MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();
            return heapMemoryUsage.getUsed() + nonHeapMemoryUsage.getUsed();
        }
    }

    // 修改判断方法（保持不变）
    private static boolean isBus(int vt) {
        return vt == 1 || vt == 3 || vt == 7 || vt == 15;
    }

    private static boolean isTrack(int vt) {
        return vt == 2 || vt == 10 || vt == 8 || vt == 11 || vt == 170 || vt == 171 || vt == 172 ||
                vt == 173 || vt == 174 || vt == 175 || vt == 176 || vt == 177;
    }

    private static int getVehicleClass(int originalType) {
        if ((originalType >= 1 && originalType <= 4) || originalType == 7 || (originalType >= 12 && originalType <= 16)) {
            return 0;
        }
        if (originalType == 8 || originalType == 10 || originalType == 11 ||
                (originalType >= 170 && originalType <= 177)) {
            return 1;
        }
        return -1;
    }

    private static String getStakeMarkByMileage(double mileage) {
        int mileageInt = (int) mileage;
        for (RoadSection section : ROAD_SECTIONS) {
            if (mileageInt >= section.startMileage && mileageInt < section.endMileage) {
                int stakeKm = section.startMileage / 1000;
                return "K" + stakeKm;
            }
        }
        return "未知桩号";
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 打印初始内存使用情况
        MemoryMonitor.printMemoryUsage("初始内存状态");

        // ==================== 主路数据处理 ====================
        String brokers = "192.168.0.5:9092";
        String groupId = "hourly-traffic-group";
        List<String> mainRoadTopics = Arrays.asList(
                "fiberData1","fiberData2","fiberData3","fiberData4","fiberData5",
                "fiberData6","fiberData7","fiberData8","fiberData9","fiberData10","fiberData11"
        );

        KafkaSource<String> mainRoadKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(mainRoadTopics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> mainRoadSourceStream = env.fromSource(
                mainRoadKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Main Road Kafka Source"
        );

        // 添加内存监控的FlatMap
        SingleOutputStreamOperator<PathPoint> mainRoadPathPointStream = mainRoadSourceStream
                .flatMap(new FlatMapFunction<String, PathPoint>() {
                    private long processedCount = 0;

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
                            processedCount++;
                            if (processedCount % 10000 == 0) {
                                MemoryMonitor.printMemoryUsage("主路数据处理中");
                            }
                        } catch (Exception e) {
                            System.err.println("Error parsing JSON: " + e.getMessage());
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PathPoint>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) ->
                                        Utils.convertToTimestampMillis(event.getTimeStamp()))
                )
                .name("MainRoadPathPointStream");

        // ==================== 匝道数据处理 ====================
        String rampGroupId = "ramp-traffic-group1";
        List<String> rampTopics = Arrays.asList("MergedRampPathData");

        KafkaSource<String> rampKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(rampTopics)
                .setGroupId(rampGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rampSourceStream = env.fromSource(
                rampKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Ramp Kafka Source"
        );

        SingleOutputStreamOperator<PathPoint> rampPathPointStream = rampSourceStream
                .flatMap(new FlatMapFunction<String, PathPoint>() {
                    private long processedCount = 0;

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
                            processedCount++;
                            if (processedCount % 1000 == 0) {
                                MemoryMonitor.printMemoryUsage("匝道数据处理中");
                            }
                        } catch (Exception e) {
                            System.err.println("Error parsing JSON: "+ e.getMessage());
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<PathPoint>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) ->
                                        Utils.convertToTimestampMillis(event.getTimeStamp()))
                )
                .name("RampPathPointStream");

        // ==================== 主路交通量统计（按小时和方向）====================
        DataStream<Tuple3<String, Integer, Integer>> totalTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple3<String, Long, Integer>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple3<String, Long, Integer>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = Utils.convertToTimestampMillis(point.getTimeStamp());
                            String hourKey = new SimpleDateFormat("yyyyMMddHH").format(eventTime);
                            out.collect(new Tuple3<>(hourKey, point.getId(), point.getDirection()));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new TotalTrafficAggregator())
                .name("TotalTrafficStream");

        totalTrafficStream.print("Total Traffic");

        // 写入总交通量HBase表（带监控）
        totalTrafficStream.addSink(new TotalHBaseTrafficSink())
                .name("TotalHBaseSink");

        // ==================== 主路详细交通量统计 ====================
        DataStream<Tuple6<String, String, Integer, Integer, Integer, Integer>> detailedTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple6<String, String, Integer, Long, Integer, Integer>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple6<String, String, Integer, Long, Integer, Integer>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = Utils.convertToTimestampMillis(point.getTimeStamp());
                            String hourKey = new SimpleDateFormat("yyyyMMddHH").format(eventTime);
                            String stakeMark = getStakeMarkByMileage(point.getMileage());
                            int vehicleType = point.getVehicleType();
                            int isBus = isBus(vehicleType) ? 1 : 0;
                            int isTrack = isTrack(vehicleType) ? 1 : 0;
                            out.collect(new Tuple6<>(hourKey, stakeMark, point.getDirection(), point.getId(), isBus, isTrack));
                        }
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1 + "_" + t.f2)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new DetailedTrafficAggregator())
                .name("DetailedTrafficStream");

        detailedTrafficStream.addSink(new DetailedHBaseTrafficSink())
                .name("DetailedHBaseSink");

        // ==================== 匝道交通量统计 ====================
        DataStream<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> rampTrafficStream = rampPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple7<String, String, Long, Integer, Double, Integer, Integer>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple7<String, String, Long, Integer, Double, Integer, Integer>> out) {
                        if (point.getStakeId() != null && point.getStakeId().contains("-")) {
                            String[] parts = point.getStakeId().split("-");
                            if (parts.length >= 2) {
                                String rampCode = parts[1].substring(0, 1);
                                if (rampCode.matches("[A-D]")) {
                                    long eventTime = Utils.convertToTimestampMillis(point.getTimeStamp());
                                    String hourKey = new SimpleDateFormat("yyyyMMddHH").format(eventTime);
                                    int vehicleClass = getVehicleClass(point.getOriginalType());
                                    int isBus = (vehicleClass == 0) ? 1 : 0;
                                    int isTrack = (vehicleClass == 1) ? 1 : 0;
                                    out.collect(new Tuple7<>(hourKey, rampCode, point.getId(), isBus, point.getSpeed(), isTrack, 1));
                                }
                            }
                        }
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new RampTrafficAggregator())
                .name("RampTrafficStream");

        rampTrafficStream.addSink(new RampHBaseTrafficSink())
                .name("RampHBaseSink");

        // ==================== 每日去重统计 ====================
        DataStream<Tuple3<String, Integer, Integer>> dailyTotalTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple4<String, Long, Integer, Long>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple4<String, Long, Integer, Long>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = Utils.convertToTimestampMillis(point.getTimeStamp());
                            String dayKey = new SimpleDateFormat("yyyyMMdd").format(eventTime);
                            out.collect(new Tuple4<>(dayKey, point.getId(), point.getDirection(), eventTime));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new DailyTotalTrafficAggregator())
                .name("DailyTotalTrafficStream");

        dailyTotalTrafficStream.addSink(new DailyTotalHBaseTrafficSink())
                .name("DailyTotalHBaseSink");

        // 每日详细交通量统计
        DataStream<Tuple6<String, String, Integer, Integer, Integer, Integer>> dailyDetailedTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple7<String, String, Integer, Long, Integer, Integer, Long>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple7<String, String, Integer, Long, Integer, Integer, Long>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = Utils.convertToTimestampMillis(point.getTimeStamp());
                            String dayKey = new SimpleDateFormat("yyyyMMdd").format(eventTime);
                            String stakeMark = getStakeMarkByMileage(point.getMileage());
                            int vehicleType = point.getVehicleType();
                            int isBus = isBus(vehicleType) ? 1 : 0;
                            int isTrack = isTrack(vehicleType) ? 1 : 0;
                            out.collect(new Tuple7<>(dayKey, stakeMark, point.getDirection(), point.getId(), isBus, isTrack, eventTime));
                        }
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1 + "_" + t.f2)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new DailyDetailedTrafficAggregator())
                .name("DailyDetailedTrafficStream");

        dailyDetailedTrafficStream.addSink(new DailyDetailedHBaseTrafficSink())
                .name("DailyDetailedHBaseSink");

        // 添加定时打印统计信息的线程
        Thread statsThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(60000); // 每分钟打印一次
                    System.out.println("\n========== 存储任务统计信息 ==========");
                    System.out.println("总内存使用: " + (MemoryMonitor.getTotalMemoryUsage() / (1024 * 1024)) + " MB");

                    for (Map.Entry<String, TableStats> entry : tableStatistics.entrySet()) {
                        System.out.println("\n表: " + entry.getKey());
                        entry.getValue().printStats();
                    }

                    System.out.println("===================================\n");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        statsThread.setDaemon(true);
        statsThread.start();

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

    // ==================== 增强的HBase Sink实现 ====================
    // 总交通量Sink（带监控）
    private static class TotalHBaseTrafficSink extends RichSinkFunction<Tuple3<String, Integer, Integer>> {
        private Connection connection;
        private Table table;
        private TableStats stats;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.0.5,192.168.0.7,192.168.0.8:,192.168.0.9,192.168.0.11,192.168.0.12");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);

            createTableIfNotExists(TABLE_NAME_TOTAL, connection);
            table = connection.getTable(TableName.valueOf(TABLE_NAME_TOTAL));
            stats = tableStatistics.computeIfAbsent(TABLE_NAME_TOTAL, k -> new TableStats());

            // 打印表结构内存估算
            System.out.println("表 " + TABLE_NAME_TOTAL + " 初始化完成，预估每行内存占用: 约200字节");
        }

        @Override
        public void invoke(Tuple3<String, Integer, Integer> value, Context context) throws Exception {
            long startTime = System.currentTimeMillis();

            String rowKey = value.f0;
            int upCount = value.f1;
            int downCount = value.f2;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upcount"),
                    Bytes.toBytes(String.valueOf(upCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downcount"),
                    Bytes.toBytes(String.valueOf(downCount)));

            table.put(put);

            long endTime = System.currentTimeMillis();
            long latency = endTime - startTime;

            // 计算存储空间
            long storageSize = calculatePutSize(put);

            // 记录统计
            stats.recordWrite(latency, storageSize);

            // 更新总统计
            totalStorageSize.addAndGet(storageSize);

            // 打印详细信息（可选择性开启）
            if (stats.writeCount.get() % 100 == 0) {
                System.out.printf("TotalTraffic - RowKey: %s, Up: %d, Down: %d, Latency: %d ms, Size: %d bytes%n",
                        rowKey, upCount, downCount, latency, storageSize);
            }
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();

            // 打印最终统计
            System.out.println("\n=== TotalHBaseTrafficSink 最终统计 ===");
            stats.printStats();
        }
    }

    // 详细交通量Sink（带监控）
    private static class DetailedHBaseTrafficSink extends RichSinkFunction<Tuple6<String, String, Integer, Integer, Integer, Integer>> {
        private Connection connection;
        private Table table;
        private TableStats stats;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.0.5,192.168.0.7,192.168.0.8:,192.168.0.9,192.168.0.11,192.168.0.12");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);

            createTableIfNotExists(TABLE_NAME_DETAIL, connection);
            table = connection.getTable(TableName.valueOf(TABLE_NAME_DETAIL));
            stats = tableStatistics.computeIfAbsent(TABLE_NAME_DETAIL, k -> new TableStats());

            System.out.println("表 " + TABLE_NAME_DETAIL + " 初始化完成，预估每行内存占用: 约300字节");
        }

        @Override
        public void invoke(Tuple6<String, String, Integer, Integer, Integer, Integer> value, Context context) throws Exception {
            long startTime = System.currentTimeMillis();

            String rowKey = value.f1 + "_" + value.f0 + "_" + value.f2;
            int busCount = value.f3;
            int trackCount = value.f4;
            int otherCount = value.f5;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("bus_count"),
                    Bytes.toBytes(String.valueOf(busCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("track_count"),
                    Bytes.toBytes(String.valueOf(trackCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("other_count"),
                    Bytes.toBytes(String.valueOf(otherCount)));

            table.put(put);

            long endTime = System.currentTimeMillis();
            long latency = endTime - startTime;

            long storageSize = calculatePutSize(put);
            stats.recordWrite(latency, storageSize);
            totalStorageSize.addAndGet(storageSize);

            if (stats.writeCount.get() % 100 == 0) {
                System.out.printf("DetailedTraffic - RowKey: %s, Bus: %d, Track: %d, Other: %d, Latency: %d ms, Size: %d bytes%n",
                        rowKey, busCount, trackCount, otherCount, latency, storageSize);
            }
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();

            System.out.println("\n=== DetailedHBaseTrafficSink 最终统计 ===");
            stats.printStats();
        }
    }

    // 匝道交通量Sink（带监控）
    private static class RampHBaseTrafficSink extends RichSinkFunction<Tuple7<String, String, Integer, Integer, Integer, Double, Integer>> {
        private Connection connection;
        private Table table;
        private TableStats stats;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.0.5,192.168.0.7,192.168.0.8:,192.168.0.9,192.168.0.11,192.168.0.12");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);

            createTableIfNotExists(TABLE_NAME_RAMP, connection);
            table = connection.getTable(TableName.valueOf(TABLE_NAME_RAMP));
            stats = tableStatistics.computeIfAbsent(TABLE_NAME_RAMP, k -> new TableStats());

            System.out.println("表 " + TABLE_NAME_RAMP + " 初始化完成，预估每行内存占用: 约400字节");
        }

        @Override
        public void invoke(Tuple7<String, String, Integer, Integer, Integer, Double, Integer> value, Context context) throws Exception {
            long startTime = System.currentTimeMillis();

            String rowKey = value.f0 + "_" + value.f1;
            int totalCount = value.f2;
            int busCount = value.f3;
            int trackCount = value.f4;
            double avgSpeed = value.f5;
            int allCount = value.f6;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("total_count"),
                    Bytes.toBytes(String.valueOf(totalCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("bus_count"),
                    Bytes.toBytes(String.valueOf(busCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("track_count"),
                    Bytes.toBytes(String.valueOf(trackCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("avg_speed"),
                    Bytes.toBytes(String.valueOf(avgSpeed)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("all_count"),
                    Bytes.toBytes(String.valueOf(allCount)));

            table.put(put);

            long endTime = System.currentTimeMillis();
            long latency = endTime - startTime;

            long storageSize = calculatePutSize(put);
            stats.recordWrite(latency, storageSize);
            totalStorageSize.addAndGet(storageSize);

            if (stats.writeCount.get() % 100 == 0) {
                System.out.printf("RampTraffic - RowKey: %s, Total: %d, Bus: %d, Track: %d, AvgSpeed: %.2f, Latency: %d ms, Size: %d bytes%n",
                        rowKey, totalCount, busCount, trackCount, avgSpeed, latency, storageSize);
            }
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();

            System.out.println("\n=== RampHBaseTrafficSink 最终统计 ===");
            stats.printStats();
        }
    }

    // 每日总交通量Sink（带监控）
    private static class DailyTotalHBaseTrafficSink extends RichSinkFunction<Tuple3<String, Integer, Integer>> {
        private Connection connection;
        private Table table;
        private TableStats stats;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.0.5,192.168.0.7,192.168.0.8:,192.168.0.9,192.168.0.11,192.168.0.12");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);

            createTableIfNotExists(TABLE_NAME_DAILY_TOTAL, connection);
            table = connection.getTable(TableName.valueOf(TABLE_NAME_DAILY_TOTAL));
            stats = tableStatistics.computeIfAbsent(TABLE_NAME_DAILY_TOTAL, k -> new TableStats());

            System.out.println("表 " + TABLE_NAME_DAILY_TOTAL + " 初始化完成，预估每行内存占用: 约200字节");
        }

        @Override
        public void invoke(Tuple3<String, Integer, Integer> value, Context context) throws Exception {
            long startTime = System.currentTimeMillis();

            String rowKey = value.f0;
            int upCount = value.f1;
            int downCount = value.f2;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upcount"),
                    Bytes.toBytes(String.valueOf(upCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downcount"),
                    Bytes.toBytes(String.valueOf(downCount)));

            table.put(put);

            long endTime = System.currentTimeMillis();
            long latency = endTime - startTime;

            long storageSize = calculatePutSize(put);
            stats.recordWrite(latency, storageSize);
            totalStorageSize.addAndGet(storageSize);

            if (stats.writeCount.get() % 10 == 0) { // 每日数据较少，每10条打印一次
                System.out.printf("DailyTotal - RowKey: %s, Up: %d, Down: %d, Latency: %d ms, Size: %d bytes%n",
                        rowKey, upCount, downCount, latency, storageSize);
            }
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();

            System.out.println("\n=== DailyTotalHBaseTrafficSink 最终统计 ===");
            stats.printStats();
        }
    }

    // 每日详细交通量Sink（带监控）
    private static class DailyDetailedHBaseTrafficSink extends RichSinkFunction<Tuple6<String, String, Integer, Integer, Integer, Integer>> {
        private Connection connection;
        private Table table;
        private TableStats stats;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.0.5,192.168.0.7,192.168.0.8:,192.168.0.9,192.168.0.11,192.168.0.12");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);

            createTableIfNotExists(TABLE_NAME_DAILY_DETAIL, connection);
            table = connection.getTable(TableName.valueOf(TABLE_NAME_DAILY_DETAIL));
            stats = tableStatistics.computeIfAbsent(TABLE_NAME_DAILY_DETAIL, k -> new TableStats());

            System.out.println("表 " + TABLE_NAME_DAILY_DETAIL + " 初始化完成，预估每行内存占用: 约300字节");
        }

        @Override
        public void invoke(Tuple6<String, String, Integer, Integer, Integer, Integer> value, Context context) throws Exception {
            long startTime = System.currentTimeMillis();

            String rowKey = value.f0 + "_" + value.f1 + "_" + value.f2;
            int busCount = value.f3;
            int trackCount = value.f4;
            int otherCount = value.f5;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("bus_count"),
                    Bytes.toBytes(String.valueOf(busCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("track_count"),
                    Bytes.toBytes(String.valueOf(trackCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("other_count"),
                    Bytes.toBytes(String.valueOf(otherCount)));

            table.put(put);

            long endTime = System.currentTimeMillis();
            long latency = endTime - startTime;

            long storageSize = calculatePutSize(put);
            stats.recordWrite(latency, storageSize);
            totalStorageSize.addAndGet(storageSize);

            if (stats.writeCount.get() % 10 == 0) {
                System.out.printf("DailyDetailed - RowKey: %s, Bus: %d, Track: %d, Other: %d, Latency: %d ms, Size: %d bytes%n",
                        rowKey, busCount, trackCount, otherCount, latency, storageSize);
            }
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();

            System.out.println("\n=== DailyDetailedHBaseTrafficSink 最终统计 ===");
            stats.printStats();
        }
    }


    // 计算Put对象的大小
    private static long calculatePutSize(Put put) {
        long size = 0;

        // RowKey大小
        size += put.getRow().length;

        // 每个单元格的大小
        for (List<Cell> cells : put.getFamilyCellMap().values()) {
            for (Cell cell : cells) {
                // RowKey (已计算)
                // Column Family
                size += cell.getFamilyLength();
                // Column Qualifier
                size += cell.getQualifierLength();
                // Value
                size += cell.getValueLength();
                // Timestamp (8字节)
                size += 8;
                // 其他开销（类型、标签等）
                size += 20; // 估算额外开销
            }
        }

        return size;
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
                    acc.busCount.get(), acc.trackCount.get(), acc.otherCount.get());
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
        public final AtomicInteger trackCount = new AtomicInteger(0);
        public final AtomicInteger otherCount = new AtomicInteger(0);

        public void addVehicle(long vehicleId, int isBus, int isTrack) {
            if (!vehicleIds.contains(vehicleId)) {
                vehicleIds.add(vehicleId);
                if (isBus == 1) {
                    busCount.incrementAndGet();
                } else if (isTrack == 1) {
                    trackCount.incrementAndGet();
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
                    trackCount.addAndGet(other.trackCount.get());
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
                    acc.busCount.get(), acc.trackCount.get(), avgSpeed, acc.totalCount.get());
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
        public final AtomicInteger trackCount = new AtomicInteger(0);
        public final AtomicInteger vehicleCount = new AtomicInteger(0);
        public final AtomicInteger totalCount = new AtomicInteger(0);
        public final AtomicDouble totalSpeed = new AtomicDouble(0.0);

        public void addVehicle(long vehicleId, int isBus, double speed, int isTrack, int count) {
            totalCount.addAndGet(count);
            totalSpeed.addAndGet(speed);

            if (!vehicleIds.contains(vehicleId)) {
                vehicleIds.add(vehicleId);
                vehicleCount.incrementAndGet();
                if (isBus == 1) {
                    busCount.incrementAndGet();
                } else if (isTrack == 1) {
                    trackCount.incrementAndGet();
                }
            }
        }

        public void merge(RampTrafficAccumulator other) {
            for (Long id : other.vehicleIds) {
                if (!vehicleIds.contains(id)) {
                    vehicleIds.add(id);
                    vehicleCount.addAndGet(1);
                    busCount.addAndGet(other.busCount.get());
                    trackCount.addAndGet(other.trackCount.get());
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
                    acc.busCount, acc.trackCount, acc.otherCount);
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
        public int trackCount = 0;
        public int otherCount = 0;
        // 临时存储车辆类型信息
        private Map<Long, Integer> vehicleBusMap = new HashMap<>();
        private Map<Long, Integer> vehicleTrackMap = new HashMap<>();

        public void addVehicle(long vehicleId, int isBus, int isTrack, long timestamp) {
            // 存储车辆类型信息
            vehicleBusMap.put(vehicleId, isBus);
            vehicleTrackMap.put(vehicleId, isTrack);

            // 将时间戳转换为两小时窗口的起始时间字符串
            String twoHourKey = getTwoHourWindowKey(timestamp);
            Set<Long> vehicleSet = twoHourWindows.computeIfAbsent(twoHourKey, k -> new HashSet<>());

            if (!vehicleSet.contains(vehicleId)) {
                vehicleSet.add(vehicleId);
                if (isBus == 1) {
                    busCount++;
                } else if (isTrack == 1) {
                    trackCount++;
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
            trackCount = 0;
            otherCount = 0;

            for (Set<Long> vehicleSet : twoHourWindows.values()) {
                for (Long vehicleId : vehicleSet) {
                    Integer isBus = vehicleBusMap.get(vehicleId);
                    Integer isTrack = vehicleTrackMap.get(vehicleId);

                    if (isBus != null && isBus == 1) {
                        busCount++;
                    } else if (isTrack != null && isTrack == 1) {
                        trackCount++;
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

    // ==================== 通用工具方法 ====================
    private static void createTableIfNotExists(String tableName, Connection connection) {
        tableLock.lock();
        try (Admin admin = connection.getAdmin()) {
            TableName hbaseTableName = TableName.valueOf(tableName);

            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

            synchronized (lock) {
                if (!admin.tableExists(hbaseTableName)) {
                    HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                    tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
                    try {
                        admin.createTable(tableDescriptor);
                        System.out.println("Table created: " + tableName);
                    } catch (TableExistsException e) {
                        // 处理表已存在但未检测到的情况
                        System.out.println("Table already exists: " + tableName);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            tableLock.unlock();
        }
    }
}