package whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import whu.edu.ljj.flink.xiaohanying.Utils.PathPoint;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import com.alibaba.fastjson2.JSONArray;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import whu.edu.ljj.flink.xiaohanying.Utils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class hourlyJobDayAndYear {
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
    private static final ReentrantLock tableLock = new ReentrantLock();

    // 表名常量
    private static final String TABLE_NAME_TOTAL = "traffic_stats";
    private static final String TABLE_NAME_DETAIL = "traffic_stats_by_stake";
    private static final String TABLE_NAME_SUMMARY = "traffic_summary";
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

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Kafka配置
        String brokers = "10.48.53.82:9092";
        String groupId = "hourly-traffic-group";
        List<String> topics = Arrays.asList("MergedPathData.sceneTest.1",
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

        // 创建Kafka源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 主数据流
        DataStream<String> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // 解析JSON为PathPoint对象
        DataStream<PathPoint> pathPointStream = sourceStream
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
                );

        // ==================== 总交通量统计（按小时和方向）====================
        DataStream<Tuple3<String, Integer, Integer>> totalTrafficStream = pathPointStream
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
                .aggregate(new TotalTrafficAggregator());

        // 写入总交通量HBase表
        totalTrafficStream.addSink(new TotalHBaseTrafficSink());

        // ==================== 详细交通量统计（按小时、桩号、方向和类型）====================
        DataStream<Tuple6<String, String, Integer, Integer, Integer, Integer>> detailedTrafficStream = pathPointStream
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
                .aggregate(new DetailedTrafficAggregator());

        // 写入详细交通量HBase表
        detailedTrafficStream.addSink(new DetailedHBaseTrafficSink());

        // ==================== 新增：每日总流量统计 ====================
        DataStream<Tuple3<String, Integer, Integer>> dailyTotalStream = totalTrafficStream
                .flatMap(new FlatMapFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple3<String, Integer, Integer>> out) {
                        // 提取日期部分 (yyyyMMdd)
                        String dateKey = value.f0.substring(0, 8);
                        out.collect(Tuple3.of(dateKey, value.f1, value.f2));
                    }
                })
                .keyBy(t -> t.f0) // 按日期分组
                .window(TumblingEventTimeWindows.of(Time.days(1))) // 1天滚动窗口
                .aggregate(new DailyTrafficAggregator());

        // ==================== 新增：年初至今累计流量统计 ====================
        DataStream<Tuple3<String, Integer, Integer>> ytdStream = dailyTotalStream
                .keyBy(t -> "YTD_" + Calendar.getInstance().get(Calendar.YEAR)) // 按年份分组
                .process(new YearToDateTrafficProcessor());

        // ==================== 合并流量统计并写入汇总表 ====================
        DataStream<TrafficSummary> summaryStream = dailyTotalStream
                .connect(ytdStream)
                .flatMap(new TrafficSummaryMerger());

        // 写入流量汇总表
        summaryStream.addSink(new SummaryHBaseTrafficSink());

        env.execute("Combined Hourly Traffic Analysis");
    }

    // ==================== 新增：每日流量聚合器 ====================
    private static class DailyTrafficAggregator implements AggregateFunction<
            Tuple3<String, Integer, Integer>,
            DailyTrafficAccumulator,
            Tuple3<String, Integer, Integer>> {

        @Override
        public DailyTrafficAccumulator createAccumulator() {
            return new DailyTrafficAccumulator();
        }

        @Override
        public DailyTrafficAccumulator add(Tuple3<String, Integer, Integer> value, DailyTrafficAccumulator acc) {
            if (acc.dateKey == null) {
                acc.dateKey = value.f0;
            }
            acc.upCount.addAndGet(value.f1);
            acc.downCount.addAndGet(value.f2);
            return acc;
        }

        @Override
        public Tuple3<String, Integer, Integer> getResult(DailyTrafficAccumulator acc) {
            return Tuple3.of(acc.dateKey, acc.upCount.get(), acc.downCount.get());
        }

        @Override
        public DailyTrafficAccumulator merge(DailyTrafficAccumulator a, DailyTrafficAccumulator b) {
            a.upCount.addAndGet(b.upCount.get());
            a.downCount.addAndGet(b.downCount.get());
            return a;
        }
    }

    private static class DailyTrafficAccumulator {
        public String dateKey;
        public final AtomicInteger upCount = new AtomicInteger(0);
        public final AtomicInteger downCount = new AtomicInteger(0);
    }

    // ==================== 新增：年初至今流量处理器 ====================
    private static class YearToDateTrafficProcessor extends KeyedProcessFunction<String, Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> {
        private ValueState<Tuple3<String, Integer, Integer>> ytdState;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            ValueStateDescriptor<Tuple3<String, Integer, Integer>> descriptor =
                    new ValueStateDescriptor<>("ytdTraffic", TypeInformation.of(new TypeHint<Tuple3<String, Integer, Integer>>() {}));
            ytdState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple3<String, Integer, Integer> dailyData, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            Tuple3<String, Integer, Integer> currentYtd = ytdState.value();
            if (currentYtd == null) {
                currentYtd = Tuple3.of("YTD_" + Calendar.getInstance().get(Calendar.YEAR), 0, 0);
            }

            // 更新累计值
            int newUpCount = currentYtd.f1 + dailyData.f1;
            int newDownCount = currentYtd.f2 + dailyData.f2;
            Tuple3<String, Integer, Integer> updatedYtd = Tuple3.of(currentYtd.f0, newUpCount, newDownCount);

            ytdState.update(updatedYtd);
            out.collect(updatedYtd);
        }
    }

    // ==================== 修改：流量汇总合并器 ====================
    private static class TrafficSummaryMerger implements
            CoFlatMapFunction<
                                Tuple3<String, Integer, Integer>,  // 第一个输入流类型 (每日流量)
                                Tuple3<String, Integer, Integer>,  // 第二个输入流类型 (年初至今流量)
                                TrafficSummary> {                   // 输出类型

        private final Map<String, Tuple3<String, Integer, Integer>> dailyCache = new HashMap<>();
        private Tuple3<String, Integer, Integer> ytdData = null;

        @Override
        public void flatMap1(Tuple3<String, Integer, Integer> dailyData, Collector<TrafficSummary> out) {
            // 处理每日流量数据
            dailyCache.put(dailyData.f0, dailyData);
            generateSummaryIfReady(out);
        }

        @Override
        public void flatMap2(Tuple3<String, Integer, Integer> ytdData, Collector<TrafficSummary> out) {
            // 处理年初至今流量数据
            this.ytdData = ytdData;
            generateSummaryIfReady(out);
        }

        private void generateSummaryIfReady(Collector<TrafficSummary> out) {
            // 当两者都有数据时，生成汇总对象
            if (ytdData != null && !dailyCache.isEmpty()) {
                for (Map.Entry<String, Tuple3<String, Integer, Integer>> entry : dailyCache.entrySet()) {
                    TrafficSummary summary = new TrafficSummary();
                    summary.dateKey = entry.getKey();
                    summary.dailyUpCount = entry.getValue().f1;
                    summary.dailyDownCount = entry.getValue().f2;
                    summary.ytdUpCount = ytdData.f1;
                    summary.ytdDownCount = ytdData.f2;
                    out.collect(summary);
                }
            }
        }
    }

    // ==================== 新增：流量汇总对象 ====================
    private static class TrafficSummary {
        public String dateKey;
        public int dailyUpCount;
        public int dailyDownCount;
        public int ytdUpCount;
        public int ytdDownCount;
    }

    // ==================== 新增：流量汇总HBase Sink ====================
    private static class SummaryHBaseTrafficSink extends RichSinkFunction<TrafficSummary> {
        private Connection connection;
        private Table table;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);

            createSummaryTableIfNotExists();
            table = connection.getTable(TableName.valueOf(TABLE_NAME_SUMMARY));
        }

        @Override
        public void invoke(TrafficSummary summary, Context context) throws Exception {
            // 写入每日流量
            Put dailyPut = new Put(Bytes.toBytes(summary.dateKey));
            dailyPut.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("daily_upcount"), Bytes.toBytes(String.valueOf(summary.dailyUpCount)));
            dailyPut.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("daily_downcount"), Bytes.toBytes(String.valueOf(summary.dailyDownCount)));
            table.put(dailyPut);

            // 写入年初至今流量
            String ytdKey = "YTD_" + Calendar.getInstance().get(Calendar.YEAR);
            Put ytdPut = new Put(Bytes.toBytes(ytdKey));
            ytdPut.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("ytd_upcount"), Bytes.toBytes(String.valueOf(summary.ytdUpCount)));
            ytdPut.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("ytd_downcount"), Bytes.toBytes(String.valueOf(summary.ytdDownCount)));
            table.put(ytdPut);

            System.out.println("Updated traffic summary: " + summary.dateKey +
                    " - Daily: Up=" + summary.dailyUpCount + ", Down=" + summary.dailyDownCount +
                    " | YTD: Up=" + summary.ytdUpCount + ", Down=" + summary.ytdDownCount);
        }

        @Override
        public void close() throws Exception {
            if (table != null) table.close();
            if (connection != null) connection.close();
        }

        private void createSummaryTableIfNotExists() throws IOException {
            tableLock.lock();
            try (Admin admin = connection.getAdmin()) {
                TableName hbaseTableName = TableName.valueOf(TABLE_NAME_SUMMARY);

                Object lock = tableCreationLocks.computeIfAbsent(TABLE_NAME_SUMMARY, k -> new Object());

                synchronized (lock) {
                    if (!admin.tableExists(hbaseTableName)) {
                        HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                        tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
                        try {
                            admin.createTable(tableDescriptor);
                            System.out.println("Table created: " + TABLE_NAME_SUMMARY);
                        } catch (TableExistsException e) {
                            System.out.println("Table already exists: " + TABLE_NAME_SUMMARY);
                        }
                    }
                }
            } finally {
                tableLock.unlock();
            }
        }
    }

    // ==================== 原有代码保持不变 ====================
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

    private static class TotalHBaseTrafficSink extends RichSinkFunction<Tuple3<String, Integer, Integer>> {
        private Connection connection;
        private Table table;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
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

    private static class DetailedHBaseTrafficSink extends RichSinkFunction<Tuple6<String, String, Integer, Integer, Integer, Integer>> {
        private Connection connection;
        private Table table;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
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
