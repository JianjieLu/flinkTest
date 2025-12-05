package whu.edu.moniDataXinghu.ingest.redisAndHbase;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
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
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class hourlyJobWithZa {
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
    private static final ReentrantLock tableLock = new ReentrantLock();

    // 表名常量
    private static final String TABLE_NAME_TOTAL = "traffic_stats";
    private static final String TABLE_NAME_DETAIL = "traffic_stats_by_stake";
//    private static final String TABLE_NAME_RAMP = "ramp_traffic_stats";
    private static final String COLUMN_FAMILY = "stats";

    // 判断客车类型的方法
    private static boolean isBus(int vt) {
        return vt == 1 || vt == 3 || vt == 7 || vt == 15;
    }

    // 判断货车类型的方法
    private static boolean isTrack(int vt) {
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

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // ==================== 主路数据处理 ====================
        // Kafka配置 - 主路数据
        String brokers = "192.168.0.5:9092";
        String groupId = "hourly-traffic-group";
        List<String> mainRoadTopics = Arrays.asList("MergedPathData.sceneTest.1",
                "MergedPathData.sceneTest.2",
                "MergedPathData.sceneTest.3",
                "MergedPathData.sceneTest.4",
                "MergedPathData.sceneTest.5",
                "MergedPathData.sceneTest.6",
                "MergedPathData.sceneTest.7",
                "MergedPathData.sceneTest.8",
                "MergedPathData.sceneTest.9",
                "MergedPathData.sceneTest.10",
                "MergedPathData.sceneTest.11");

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

                            for (int i = 0; i < pathList.size(); i++) {
                                PathPoint point = pathList.getObject(i, PathPoint.class);
                                point.setTimeStamp(timestamp);
                                out.collect(point);
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

        // ==================== 主路交通量统计（按小时和方向）====================
        DataStream<Tuple3<String, Integer, Integer>> totalTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple3<String, Long, Integer>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple3<String, Long, Integer>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = Utils.convertToTimestampMillis(point.getTimeStamp());
                            String hourKey = new SimpleDateFormat("yyyyMMddHH").format(eventTime);
                            out.collect(Tuple3.of(hourKey, point.getId(), point.getDirection()));
                        }
                    }
                })
                .keyBy(t -> t.f0)  // 按小时分组
                .window(TumblingEventTimeWindows.of(Time.hours(1))) // 1小时滚动窗口
                .aggregate(new TotalTrafficAggregator())
                .name("TotalTrafficStream");

        // 写入总交通量HBase表
        totalTrafficStream.addSink(new TotalHBaseTrafficSink())
                .name("TotalHBaseSink");

        // ==================== 主路详细交通量统计（按小时、桩号、方向和类型）====================
        DataStream<Tuple6<String, String, Integer, Integer, Integer, Integer>> detailedTrafficStream = mainRoadPathPointStream
                .flatMap(new FlatMapFunction<PathPoint, Tuple6<String, String, Integer, Long, Integer, Integer>>() {
                    @Override
                    public void flatMap(PathPoint point, Collector<Tuple6<String, String, Integer, Long, Integer, Integer>> out) {
                        if (point.getDirection() == 1 || point.getDirection() == 2) {
                            long eventTime = Utils.convertToTimestampMillis(point.getTimeStamp());
                            String hourKey = new SimpleDateFormat("yyyyMMddHH").format(eventTime);

                            // 计算桩号 (mileage除以1000取整)
                            int stake = (int) (point.getMileage() / 1000);
                            String stakeKey = "K" + stake;

                            // 判断车辆类型
                            int vehicleType = point.getVehicleType();
                            int isBus = isBus(vehicleType) ? 1 : 0;
                            int isTrack = isTrack(vehicleType) ? 1 : 0;

                            out.collect(Tuple6.of(hourKey, stakeKey, point.getDirection(), point.getId(), isBus, isTrack));
                        }
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1 + "_" + t.f2)  // 按小时+桩号+方向分组
                .window(TumblingEventTimeWindows.of(Time.hours(1))) // 1小时滚动窗口
                .aggregate(new DetailedTrafficAggregator())
                .name("DetailedTrafficStream");

        // 写入详细交通量HBase表
        detailedTrafficStream.addSink(new DetailedHBaseTrafficSink())
                .name("DetailedHBaseSink");


        env.execute("Combined Hourly Traffic Analysis");
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
                acc.stakeKey = value.f1;
                acc.direction = value.f2;
            }
            acc.addVehicle(value.f3, value.f4, value.f5);
            return acc;
        }

        @Override
        public Tuple6<String, String, Integer, Integer, Integer, Integer> getResult(DetailedTrafficAccumulator acc) {
            return Tuple6.of(acc.hourKey, acc.stakeKey, acc.direction,
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
        public String stakeKey;
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

    // ==================== HBase Sink 实现 ====================
    // 总交通量Sink
    private static class TotalHBaseTrafficSink extends RichSinkFunction<Tuple3<String, Integer, Integer>> {
        private Connection connection;
        private Table table;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.0.5,192.168.0.7,192.168.0.8:,192.168.0.9,192.168.0.11,192.168.0.12");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);

            createTableIfNotExists(TABLE_NAME_TOTAL, connection);
            table = connection.getTable(TableName.valueOf(TABLE_NAME_TOTAL));
        }

        @Override
        public void invoke(Tuple3<String, Integer, Integer> value, Context context) throws Exception {
            String rowKey = value.f0; // yyyyMMddHH格式
            int upCount = value.f1;
            int downCount = value.f2;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("upcount"), Bytes.toBytes(String.valueOf(upCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("downcount"), Bytes.toBytes(String.valueOf(downCount)));

            table.put(put);
            System.out.println("Inserted total traffic data: " + rowKey +
                    " - Up: " + upCount +
                    ", Down: " + downCount);
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();
        }
    }

    // 详细交通量Sink
    private static class DetailedHBaseTrafficSink extends RichSinkFunction<Tuple6<String, String, Integer, Integer, Integer, Integer>> {
        private Connection connection;
        private Table table;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.0.5,192.168.0.7,192.168.0.8:,192.168.0.9,192.168.0.11,192.168.0.12");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);

            createTableIfNotExists(TABLE_NAME_DETAIL, connection);
            table = connection.getTable(TableName.valueOf(TABLE_NAME_DETAIL));
        }

        @Override
        public void invoke(Tuple6<String, String, Integer, Integer, Integer, Integer> value, Context context) throws Exception {
            String rowKey = value.f0 + "_" + value.f1 + "_" + value.f2; // 小时_桩号_方向
            int busCount = value.f3;
            int trackCount = value.f4;
            int otherCount = value.f5;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("bus_count"), Bytes.toBytes(String.valueOf(busCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("track_count"), Bytes.toBytes(String.valueOf(trackCount)));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("other_count"), Bytes.toBytes(String.valueOf(otherCount)));

            table.put(put);
            System.out.println("Inserted detailed traffic data: " + rowKey +
                    " - Bus: " + busCount +
                    ", Track: " + trackCount +
                    ", Other: " + otherCount);
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();
        }
    }

    //

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