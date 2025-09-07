package whu.edu.moniData.ingest;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
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
public class FlinkHourlyTrafficAnalysis {
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
    private static final ReentrantLock tableLock = new ReentrantLock();
    private static final String tableName = "traffic_stats";
    private static final String columnFamily = "stats";
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

        // 每小时交通量统计
        DataStream<Tuple3<String, Integer, Integer>> hourlyTrafficStream = pathPointStream
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
                .aggregate(new TrafficAggregator());

        // 写入HBase
        hourlyTrafficStream.addSink(new HBaseTrafficSink());

        env.execute("Hourly Traffic Analysis");
    }

    // 交通量聚合函数
    private static class TrafficAggregator implements AggregateFunction<
            Tuple3<String, Long, Integer>,
            TrafficAccumulator,
            Tuple3<String, Integer, Integer>> {

        @Override
        public TrafficAccumulator createAccumulator() {
            return new TrafficAccumulator();
        }

        @Override
        public TrafficAccumulator add(Tuple3<String, Long, Integer> value, TrafficAccumulator acc) {
            if (acc.hourKey == null) {
                acc.hourKey = value.f0;
            }
            acc.addVehicle(value.f1, value.f2);
            return acc;
        }

        @Override
        public Tuple3<String, Integer, Integer> getResult(TrafficAccumulator acc) {
            return Tuple3.of(acc.hourKey, acc.upCount.get(), acc.downCount.get());
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
        public final Set<Long> vehicleIds = new HashSet<>();
        public final AtomicInteger upCount = new AtomicInteger(0);
        public final AtomicInteger downCount = new AtomicInteger(0);

        public void addVehicle(long vehicleId, int direction) {
            if (!vehicleIds.contains(vehicleId)) {
                vehicleIds.add(vehicleId);
                if (direction == 1) {
                    upCount.incrementAndGet();
                    System.out.println(upCount);
                }
                else if (direction == 2) {
                    downCount.incrementAndGet();
                    System.out.println(downCount);
                }
            }
        }

        public void merge(TrafficAccumulator other) {
            for (Long id : other.vehicleIds) {
                if (!vehicleIds.contains(id)) {
                    vehicleIds.add(id);
                    if (other.upCount.get() > 0) upCount.incrementAndGet();
                    if (other.downCount.get() > 0) downCount.incrementAndGet();
                }
            }
        }
    }

    // HBase Sink
    private static class HBaseTrafficSink extends RichSinkFunction<Tuple3<String, Integer, Integer>> {
        private Connection connection;
        private Table table;


        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);


            createTableIfNotExists(tableName,columnFamily,connection);
            table = connection.getTable(TableName.valueOf(tableName));
        }

        @Override
        public void invoke(Tuple3<String, Integer, Integer> value, Context context) throws Exception {
            String rowKey = value.f0; // yyyyMMddHH格式
            int upCount = value.f1;
            int downCount = value.f2;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("upcount"), Bytes.toBytes(String.valueOf(upCount)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("downcount"), Bytes.toBytes(String.valueOf(downCount)));

            table.put(put);
            System.out.println("Inserted traffic data: " + rowKey +
                    " - Up: " + upCount +
                    ", Down: " + downCount);
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