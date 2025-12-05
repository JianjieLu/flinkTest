package whu.edu.moniData.ingest.newIngest;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
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

public class FlinkRampHourlyAnalysis {
    private static final ConcurrentHashMap<String, Object> tableCreationLocks = new ConcurrentHashMap<>();
    private static final ReentrantLock tableLock = new ReentrantLock();
    private static final String tableName = "ramp_hourly_stats";
    private static final String columnFamily = "stats";

    // 匝道类型
    private static final Set<String> RAMP_TYPES = new HashSet<>(Arrays.asList("A", "B", "C", "D"));

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Kafka配置
        String brokers = "10.48.53.82:9092";
        String groupId = "ramp-hourly-group";
        List<String> topics = Arrays.asList("MergedRampPathData");

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

        // 解析JSON并提取匝道信息
        DataStream<RampVehicleData> rampDataStream = sourceStream
                .flatMap(new FlatMapFunction<String, RampVehicleData>() {
                    @Override
                    public void flatMap(String value, Collector<RampVehicleData> out) {
                        try {
                            JSONObject json = JSON.parseObject(value);
                            String timestamp = json.getString("timeStamp");
                            JSONArray pathList = json.getJSONArray("pathList");

                            for (int i = 0; i < pathList.size(); i++) {
                                JSONObject vehicleJson = pathList.getJSONObject(i);

                                // 提取匝道信息
                                String stakeId = vehicleJson.getString("stakeId");
                                String rampType = extractRampType(stakeId);

                                // 只处理A、B、C、D四种匝道
                                if (RAMP_TYPES.contains(rampType)) {
                                    String plateNo = vehicleJson.getString("plateNo");
                                    double speed = vehicleJson.getDoubleValue("speed");
                                    int originalType = vehicleJson.getIntValue("originalType");
                                    int vehicleType = getKeHuo(originalType);

                                    // 只处理客车和货车
                                    if (vehicleType == 0 || vehicleType == 1) {
                                        RampVehicleData data = new RampVehicleData(
                                                rampType, plateNo, speed, vehicleType, timestamp
                                        );
//                                        System.out.println("rampType"+rampType+"  plateNo:"+plateNo+"   speed:"+speed+ "   vehicleType:"+vehicleType+"  timestamp:"+timestamp);
                                        out.collect(data);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error parsing JSON: " + e.getMessage());
                        }
                    }

                    // 提取匝道类型
                    private String extractRampType(String stakeId) {
                        if (stakeId == null || !stakeId.contains("-")) {
                            return "UNKNOWN";
                        }

                        String[] parts = stakeId.split("-");
                        if (parts.length < 2) {
                            return "UNKNOWN";
                        }

                        // 提取字母部分
                        String rampPart = parts[1];
                        for (char c : rampPart.toCharArray()) {
                            if (Character.isLetter(c)) {
                                return String.valueOf(c).toUpperCase();
                            }
                        }

                        return "UNKNOWN";
                    }

                    // 区分客货车
                    private int getKeHuo(int originalType) {
                        if ((originalType >= 1 && originalType <= 4) || originalType == 7 ||
                                (originalType >= 12 && originalType <= 16)) {
                            return 0; // 客车
                        }
                        if (originalType == 8 || originalType == 10 || originalType == 11 ||
                                (originalType >= 170 && originalType <= 177)) {
                            return 1; // 货车
                        }
                        return -1; // 其他类型
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<RampVehicleData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, recordTimestamp) ->
                                        Utils.convertToTimestampMillis(event.getTimestamp()))
                );

        // 按匝道和小时进行统计
        DataStream<RampHourlyStats> rampStatsStream = rampDataStream
                .keyBy(data -> data.getRampType() + "_" +
                        new SimpleDateFormat("yyyyMMddHH").format(
                                Utils.convertToTimestampMillis(data.getTimestamp())))
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new RampAggregator());

        // 写入HBase
        rampStatsStream.addSink(new HBaseRampSink());

        env.execute("Ramp Hourly Traffic Analysis");
    }

    // 匝道车辆数据类
    public static class RampVehicleData {
        private String rampType;
        private String plateNo;
        private double speed;
        private int vehicleType; // 0:客车, 1:货车
        private String timestamp;

        public RampVehicleData(String rampType, String plateNo, double speed, int vehicleType, String timestamp) {
            this.rampType = rampType;
            this.plateNo = plateNo;
            this.speed = speed;
            this.vehicleType = vehicleType;
            this.timestamp = timestamp;
        }

        public String getRampType() { return rampType; }
        public String getPlateNo() { return plateNo; }
        public double getSpeed() { return speed; }
        public int getVehicleType() { return vehicleType; }
        public String getTimestamp() { return timestamp; }
    }

    // 匝道小时统计类
    public static class RampHourlyStats {
        private String rampType;
        private String hourKey;
        private int totalVehicles;
        private int passengerVehicles;
        private int freightVehicles;
        private double avgSpeed;

        public RampHourlyStats() {}

        public RampHourlyStats(String rampType, String hourKey, int totalVehicles,
                               int passengerVehicles, int freightVehicles, double avgSpeed) {
            this.rampType = rampType;
            this.hourKey = hourKey;
            this.totalVehicles = totalVehicles;
            this.passengerVehicles = passengerVehicles;
            this.freightVehicles = freightVehicles;
            this.avgSpeed = avgSpeed;
        }

        // Getters and setters
        public String getRampType() { return rampType; }
        public void setRampType(String rampType) { this.rampType = rampType; }

        public String getHourKey() { return hourKey; }
        public void setHourKey(String hourKey) { this.hourKey = hourKey; }

        public int getTotalVehicles() { return totalVehicles; }
        public void setTotalVehicles(int totalVehicles) { this.totalVehicles = totalVehicles; }

        public int getPassengerVehicles() { return passengerVehicles; }
        public void setPassengerVehicles(int passengerVehicles) { this.passengerVehicles = passengerVehicles; }

        public int getFreightVehicles() { return freightVehicles; }
        public void setFreightVehicles(int freightVehicles) { this.freightVehicles = freightVehicles; }

        public double getAvgSpeed() { return avgSpeed; }
        public void setAvgSpeed(double avgSpeed) { this.avgSpeed = avgSpeed; }

        @Override
        public String toString() {
            return "RampHourlyStats{" +
                    "rampType='" + rampType + '\'' +
                    ", hourKey='" + hourKey + '\'' +
                    ", totalVehicles=" + totalVehicles +
                    ", passengerVehicles=" + passengerVehicles +
                    ", freightVehicles=" + freightVehicles +
                    ", avgSpeed=" + avgSpeed +
                    '}';
        }
    }

    // 匝道聚合函数
    private static class RampAggregator implements AggregateFunction<
            RampVehicleData,
            RampAccumulator,
            RampHourlyStats> {

        @Override
        public RampAccumulator createAccumulator() {
            return new RampAccumulator();
        }

        @Override
        public RampAccumulator add(RampVehicleData value, RampAccumulator acc) {
            if (acc.rampType == null) {
                acc.rampType = value.getRampType();
                acc.hourKey = new SimpleDateFormat("yyyyMMddHH").format(
                        Utils.convertToTimestampMillis(value.getTimestamp()));
            }

            acc.addVehicle(value.getPlateNo(), value.getSpeed(), value.getVehicleType());
            return acc;
        }

        @Override
        public RampHourlyStats getResult(RampAccumulator acc) {
            return new RampHourlyStats(
                    acc.rampType,
                    acc.hourKey,
                    acc.getTotalVehicles(),
                    acc.passengerCount.get(),
                    acc.freightCount.get(),
                    acc.getAvgSpeed()
            );
        }

        @Override
        public RampAccumulator merge(RampAccumulator a, RampAccumulator b) {
            a.merge(b);
            return a;
        }
    }

    // 匝道累加器
    private static class RampAccumulator {
        public String rampType;
        public String hourKey;
        public final Set<String> vehiclePlates = new HashSet<>();
        public final AtomicInteger passengerCount = new AtomicInteger(0);
        public final AtomicInteger freightCount = new AtomicInteger(0);
        public double totalSpeed = 0.0;
        public int speedCount = 0;

        public void addVehicle(String plateNo, double speed, int vehicleType) {
            // 按车牌去重
            if (!vehiclePlates.contains(plateNo)) {
                vehiclePlates.add(plateNo);

                if (vehicleType == 0) {
                    passengerCount.incrementAndGet();
                } else if (vehicleType == 1) {
                    freightCount.incrementAndGet();
                }

                // 累加速度用于计算平均值
                totalSpeed += speed;
                speedCount++;
            }
        }

        public void merge(RampAccumulator other) {
            for (String plate : other.vehiclePlates) {
                if (!vehiclePlates.contains(plate)) {
                    vehiclePlates.add(plate);

                    if (other.passengerCount.get() > 0) {
                        passengerCount.incrementAndGet();
                    }
                    if (other.freightCount.get() > 0) {
                        freightCount.incrementAndGet();
                    }
                }
            }

            totalSpeed += other.totalSpeed;
            speedCount += other.speedCount;
        }

        public int getTotalVehicles() {
            return vehiclePlates.size();
        }

        public double getAvgSpeed() {
            return speedCount > 0 ? totalSpeed / speedCount : 0.0;
        }
    }

    // HBase Sink for ramp data
    private static class HBaseRampSink extends RichSinkFunction<RampHourlyStats> {
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
        public void invoke(RampHourlyStats stats, Context context) throws Exception {
            System.out.println("开始插入");
            String rowKey = stats.getRampType() + "_" + stats.getHourKey();

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("total_vehicles"),
                    Bytes.toBytes(String.valueOf(stats.getTotalVehicles())));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("passenger_vehicles"),
                    Bytes.toBytes(String.valueOf(stats.getPassengerVehicles())));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("freight_vehicles"),
                    Bytes.toBytes(String.valueOf(stats.getFreightVehicles())));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("avg_speed"),
                    Bytes.toBytes(String.valueOf(stats.getAvgSpeed())));
            System.out.println("String.valueOf(stats.getTotalVehicles())):"+
                    String.valueOf(stats.getTotalVehicles())+"String.valueOf(stats.getPassengerVehicles())):"+
                    String.valueOf(stats.getPassengerVehicles()));
            table.put(put);
            System.out.println("Inserted ramp stats: " + stats.toString());
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