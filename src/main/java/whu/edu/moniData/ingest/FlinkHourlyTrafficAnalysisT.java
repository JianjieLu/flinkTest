package whu.edu.moniData.ingest;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

public class FlinkHourlyTrafficAnalysisT {
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
    private static final ReentrantLock tableLock = new ReentrantLock();
    private static final String tableName = "traffic_stats_by_stake";
    private static final String columnFamily = "stats";



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

        // 每小时交通量统计，按桩号和类型区分
        DataStream<Tuple6<String, String, Integer, Integer, Integer, Integer>> hourlyTrafficStream = pathPointStream
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
                .aggregate(new TrafficAggregator());

        // 写入HBase
        hourlyTrafficStream.addSink(new HBaseTrafficSink());

        env.execute("Hourly Traffic Analysis By Stake");
    }

    // 交通量聚合函数
    private static class TrafficAggregator implements AggregateFunction<
            Tuple6<String, String, Integer, Long, Integer, Integer>,
            TrafficAccumulator,
            Tuple6<String, String, Integer, Integer, Integer, Integer>> {

        @Override
        public TrafficAccumulator createAccumulator() {
            return new TrafficAccumulator();
        }

        @Override
        public TrafficAccumulator add(Tuple6<String, String, Integer, Long, Integer, Integer> value, TrafficAccumulator acc) {
            if (acc.hourKey == null) {
                acc.hourKey = value.f0;
                acc.stakeKey = value.f1;
                acc.direction = value.f2;
            }
            acc.addVehicle(value.f3, value.f4, value.f5);
            return acc;
        }

        @Override
        public Tuple6<String, String, Integer, Integer, Integer, Integer> getResult(TrafficAccumulator acc) {
            return Tuple6.of(acc.hourKey, acc.stakeKey, acc.direction,
                    acc.busCount.get(), acc.trackCount.get(), acc.otherCount.get());
        }

        @Override
        public TrafficAccumulator merge(TrafficAccumulator a, TrafficAccumulator b) {
            a.merge(b);
            return a;
        }
    }

    // 交通量累加器
    private static class TrafficAccumulator {
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

        public void merge(TrafficAccumulator other) {
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

    // HBase Sink
    private static class HBaseTrafficSink extends RichSinkFunction<Tuple6<String, String, Integer, Integer, Integer, Integer>> {
        private Connection connection;
        private Table table;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);

            createTableIfNotExists(tableName, columnFamily, connection);
            table = connection.getTable(TableName.valueOf(tableName));
        }

        @Override
        public void invoke(Tuple6<String, String, Integer, Integer, Integer, Integer> value, Context context) throws Exception {
            String rowKey = value.f0 + "_" + value.f1 + "_" + value.f2; // 小时_桩号_方向
            int busCount = value.f3;
            int trackCount = value.f4;
            int otherCount = value.f5;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bus_count"), Bytes.toBytes(String.valueOf(busCount)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("track_count"), Bytes.toBytes(String.valueOf(trackCount)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("other_count"), Bytes.toBytes(String.valueOf(otherCount)));

            table.put(put);
            System.out.println("Inserted traffic data: " + rowKey +
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

    private static void createTableIfNotExists(String tableName, String columnFamily, Connection connection) {
        tableLock.lock();
        try (Admin admin = connection.getAdmin()) {
            TableName hbaseTableName = TableName.valueOf(tableName);

            Object lock = tableCreationLocks.computeIfAbsent(tableName, k -> new Object());

            synchronized (lock) {
                admin.listTables();
                if (!admin.tableExists(hbaseTableName)) {
                    HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                    tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
                    try {
                        admin.createTable(tableDescriptor);
                        System.out.println("Table created: " + tableName);
                    } catch (TableExistsException e) {
                        // 处理表已存在但未检测到的情况
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