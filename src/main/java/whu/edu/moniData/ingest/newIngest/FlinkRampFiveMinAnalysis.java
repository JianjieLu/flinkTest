package whu.edu.moniData.ingest.newIngest;

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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONArray;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class FlinkRampFiveMinAnalysis {
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
    private static final ReentrantLock tableLock = new ReentrantLock();
    private static final String tableName = "ramp_traffic_stats_5min";
    private static final String columnFamily = "stats";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Kafka配置
        String brokers = "10.48.53.82:9092";
        String groupId = "ramp-5min-group";
        String topic = "MergedRampPathData";

        // 创建Kafka源
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(Collections.singletonList(topic))
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

        // 解析JSON并提取匝道信息
        DataStream<Tuple6<String, String, String, Integer, Double, Long>> rampStream = sourceStream
                .flatMap(new FlatMapFunction<String, Tuple6<String, String, String, Integer, Double, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple6<String, String, String, Integer, Double, Long>> out) {
                        try {
                            JSONObject json = JSON.parseObject(value);
                            JSONArray pathList = json.getJSONArray("pathList");

                            for (int i = 0; i < pathList.size(); i++) {
                                JSONObject point = pathList.getJSONObject(i);
                                String stakeId = point.getString("stakeId");

                                // 提取匝道编号
                                String ramp = extractRampCode(stakeId);
                                if (ramp != null && (ramp.equals("A") || ramp.equals("B") || ramp.equals("C") || ramp.equals("D"))) {
                                    String plateNo = point.getString("plateNo");
                                    int originalType = point.getIntValue("originalType");
                                    double speed = point.getDoubleValue("speed");
                                    String timestampStr = point.getString("timeStamp");

                                    // 转换时间戳
                                    long timestamp = convertToTimestampMillis(timestampStr);
                                    // 生成5分钟时间块key (yyyyMMddHHmm)
                                    String fiveMinKey = generateFiveMinKey(timestamp);

                                    // 区分客货车
                                    int vehicleType = getKeHuo(originalType);
                                    if (vehicleType != -1) { // 只处理客车和货车
                                        out.collect(Tuple6.of(ramp, fiveMinKey, plateNo, vehicleType, speed, timestamp));
                                    }
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error parsing JSON: " + e.getMessage());
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple6<String, String, String, Integer, Double, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) -> event.f5)
                );

        // 每5分钟匝道交通量统计
        DataStream<Tuple6<String, String, Integer, Integer, Integer, Double>> rampFiveMinStream = rampStream
                .keyBy(t -> t.f0 + "_" + t.f1)  // 按匝道和5分钟块分组
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new RampTrafficAggregator());

        // 写入HBase
        rampFiveMinStream.addSink(new HBaseRampSink());

        env.execute("Ramp 5-Minute Traffic Analysis");
    }

    // 生成5分钟时间块key (格式: yyyyMMddHHmm)
    private static String generateFiveMinKey(long timestamp) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(timestamp);
        int minute = cal.get(Calendar.MINUTE);
        int fiveMinBlock = (minute / 5) * 5; // 计算5分钟块
        cal.set(Calendar.MINUTE, fiveMinBlock);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new SimpleDateFormat("yyyyMMddHHmm").format(cal.getTime());
    }

    // 提取匝道编号
    private static String extractRampCode(String stakeId) {
        if (stakeId == null || !stakeId.contains("-")) {
            return null;
        }

        String[] parts = stakeId.split("-");
        if (parts.length < 2) {
            return null;
        }

        String rampPart = parts[1];
        if (rampPart.length() > 0) {
            return rampPart.substring(0, 1);
        }

        return null;
    }

    // 时间戳转换方法
    private static long convertToTimestampMillis(String timestampStr) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
            return format.parse(timestampStr).getTime();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    // 区分客货车
    private static int getKeHuo(int originalType) {
        if ((originalType >= 1 && originalType <= 4) || originalType == 7 || (originalType >= 12 && originalType <= 16)) {
            return 0; // 客车
        }
        if (originalType == 8 || originalType == 10 || originalType == 11 ||
                (originalType >= 170 && originalType <= 177)) {
            return 1; // 货车
        }
        return -1; // 其他类型
    }

    // 匝道交通量聚合函数
    private static class RampTrafficAggregator implements AggregateFunction<
            Tuple6<String, String, String, Integer, Double, Long>,
            RampTrafficAccumulator,
            Tuple6<String, String, Integer, Integer, Integer, Double>> {

        @Override
        public RampTrafficAccumulator createAccumulator() {
            return new RampTrafficAccumulator();
        }

        @Override
        public RampTrafficAccumulator add(Tuple6<String, String, String, Integer, Double, Long> value,
                                          RampTrafficAccumulator acc) {
            if (acc.ramp == null) {
                acc.ramp = value.f0;
                acc.timeBlockKey = value.f1;
            }

            acc.addVehicle(value.f2, value.f3, value.f4);
            return acc;
        }

        @Override
        public Tuple6<String, String, Integer, Integer, Integer, Double> getResult(RampTrafficAccumulator acc) {
            double avgSpeed = acc.vehicleCount.get() > 0 ? acc.totalSpeed.get() / acc.vehicleCount.get() : 0.0;
            return Tuple6.of(acc.ramp, acc.timeBlockKey, acc.vehicleCount.get(),
                    acc.kecheCount.get(), acc.huocheCount.get(), avgSpeed);
        }

        @Override
        public RampTrafficAccumulator merge(RampTrafficAccumulator a, RampTrafficAccumulator b) {
            a.merge(b);
            return a;
        }
    }

    // 匝道交通量累加器
    private static class RampTrafficAccumulator {
        public String ramp;
        public String timeBlockKey;
        public final Set<String> vehiclePlates = new HashSet<>();
        public final AtomicInteger vehicleCount = new AtomicInteger(0);
        public final AtomicInteger kecheCount = new AtomicInteger(0);
        public final AtomicInteger huocheCount = new AtomicInteger(0);
        public final AtomicInteger totalSpeed = new AtomicInteger(0);

        public void addVehicle(String plateNo, int vehicleType, double speed) {
            if (!vehiclePlates.contains(plateNo)) {
                vehiclePlates.add(plateNo);
                vehicleCount.incrementAndGet();
                totalSpeed.addAndGet((int) speed);

                if (vehicleType == 0) {
                    kecheCount.incrementAndGet();
                } else if (vehicleType == 1) {
                    huocheCount.incrementAndGet();
                }
            }
        }

        public void merge(RampTrafficAccumulator other) {
            for (String plate : other.vehiclePlates) {
                if (!vehiclePlates.contains(plate)) {
                    vehiclePlates.add(plate);
                    vehicleCount.incrementAndGet();
                    // 注意：这里简化处理，实际合并时可能需要更复杂的逻辑处理速度平均值
                }
            }
            kecheCount.addAndGet(other.kecheCount.get());
            huocheCount.addAndGet(other.huocheCount.get());
            totalSpeed.addAndGet(other.totalSpeed.get());
        }
    }

    // HBase Sink for ramp data
    private static class HBaseRampSink extends RichSinkFunction<Tuple6<String, String, Integer, Integer, Integer, Double>> {
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
        public void invoke(Tuple6<String, String, Integer, Integer, Integer, Double> value, Context context) throws Exception {
            String rowKey = value.f0 + "_" + value.f1; // 匝道编号_5分钟块格式
            int totalVehicles = value.f2;
            int kecheCount = value.f3;
            int huocheCount = value.f4;
            double avgSpeed = value.f5;

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("total_vehicles"), Bytes.toBytes(String.valueOf(totalVehicles)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("keche_count"), Bytes.toBytes(String.valueOf(kecheCount)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("huoche_count"), Bytes.toBytes(String.valueOf(huocheCount)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("avg_speed"), Bytes.toBytes(String.valueOf(avgSpeed)));

            table.put(put);
            System.out.println("Inserted ramp traffic data: " + rowKey +
                    " - Total: " + totalVehicles +
                    ", Keche: " + kecheCount +
                    ", Huoche: " + huocheCount +
                    ", AvgSpeed: " + avgSpeed);
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
                if (!admin.tableExists(hbaseTableName)) {
                    HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
                    tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
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