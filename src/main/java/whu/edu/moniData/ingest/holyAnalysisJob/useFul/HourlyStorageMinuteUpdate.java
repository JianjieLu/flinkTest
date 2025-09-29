package whu.edu.moniData.ingest.holyAnalysisJob.useFul;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class HourlyStorageMinuteUpdate {

    // 定义时间格式化器
    private static final DateTimeFormatter JSON_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
    private static final DateTimeFormatter MINUTE_ROWKEY_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmm");
    private static final String TABLE_NAME = "hourly_traffic";
    private static final String COLUMN_FAMILY = "cf";

    // Kafka配置
    private static final String BROKERS = "10.48.53.82:9092";
    private static final String GROUP_ID = "hourly-storage-group";
    private static final String TOPIC = "TrafficData";

    // 车辆事件数据结构
    public static class VehicleEvent {
        private String vehicleId;
        private long timestamp;
        private String locationId;
        private int vehicleType;
        private double speed;

        public VehicleEvent(String vehicleId, long timestamp, String locationId, int vehicleType, double speed) {
            this.vehicleId = vehicleId;
            this.timestamp = timestamp;
            this.locationId = locationId;
            this.vehicleType = vehicleType;
            this.speed = speed;
        }

        public String getVehicleId() {
            return vehicleId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getLocationId() {
            return locationId;
        }

        public int getVehicleType() {
            return vehicleType;
        }

        public double getSpeed() {
            return speed;
        }
    }

    // 聚合结果数据结构
    public static class HourlyStats {
        private int totalVehicles;
        private int passengerVehicles;
        private int freightVehicles;
        private double totalSpeed;
        private int count;
        private long lastUpdateTime;

        public HourlyStats() {
            this.totalVehicles = 0;
            this.passengerVehicles = 0;
            this.freightVehicles = 0;
            this.totalSpeed = 0.0;
            this.count = 0;
            this.lastUpdateTime = System.currentTimeMillis();
        }

        public void addVehicle(VehicleEvent event) {
            totalVehicles++;
            if (isPassengerVehicle(event.getVehicleType())) {
                passengerVehicles++;
            } else if (isFreightVehicle(event.getVehicleType())) {
                freightVehicles++;
            }
            totalSpeed += event.getSpeed();
            count++;
            lastUpdateTime = System.currentTimeMillis();
        }

        public void merge(HourlyStats other) {
            totalVehicles += other.totalVehicles;
            passengerVehicles += other.passengerVehicles;
            freightVehicles += other.freightVehicles;
            totalSpeed += other.totalSpeed;
            count += other.count;
            lastUpdateTime = Math.max(lastUpdateTime, other.lastUpdateTime);
        }

        public double getAverageSpeed() {
            return count > 0 ? totalSpeed / count : 0.0;
        }

        public int getTotalVehicles() {
            return totalVehicles;
        }

        public int getPassengerVehicles() {
            return passengerVehicles;
        }

        public int getFreightVehicles() {
            return freightVehicles;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 创建Kafka源
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BROKERS)
                .setTopics(TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 使用事件时间的水位线策略，允许5分钟的延迟
        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            try {
                                JSONObject json = new JSONObject(event);
                                String timeStampStr = json.getString("timeStamp");
                                LocalDateTime dateTime = LocalDateTime.parse(timeStampStr, JSON_TIME_FORMATTER);
                                return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            } catch (JSONException e) {
                                return System.currentTimeMillis();
                            }
                        }),
                "Kafka Source"
        );

        // 解析JSON并提取车辆数据
        DataStream<VehicleEvent> vehicleEvents = kafkaStream
                .flatMap(new FlatMapFunction<String, VehicleEvent>() {
                    @Override
                    public void flatMap(String jsonString, Collector<VehicleEvent> out) {
                        try {
                            JSONObject jsonObject = new JSONObject(jsonString);
                            String timeStampStr = jsonObject.getString("timeStamp");

                            long eventTimestamp = LocalDateTime.parse(timeStampStr, JSON_TIME_FORMATTER)
                                    .atZone(ZoneId.systemDefault())
                                    .toInstant()
                                    .toEpochMilli();

                            JSONArray vehicleList = jsonObject.getJSONArray("vehicleList");
                            for (int i = 0; i < vehicleList.length(); i++) {
                                JSONObject vehicle = vehicleList.getJSONObject(i);
                                String vehicleId = vehicle.getString("id");
                                String locationId = vehicle.getString("locationId");
                                int vehicleType = vehicle.getInt("vehicleType");
                                double speed = vehicle.getDouble("speed");

                                out.collect(new VehicleEvent(
                                        vehicleId,
                                        eventTimestamp,
                                        locationId,
                                        vehicleType,
                                        speed
                                ));
                            }
                        } catch (JSONException e) {
                            System.err.println("JSON解析错误: " + e.getMessage() + "\n原始数据: " + jsonString);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VehicleEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner((event, ts) -> event.getTimestamp())
                );

        // 关键修复：确保返回 KeyedStream
        KeyedStream<VehicleEvent, String> keyedByLocation = vehicleEvents
                .keyBy(VehicleEvent::getLocationId);

        // 使用滑动窗口：窗口大小1小时，滑动间隔1分钟
        DataStream<Tuple3<String, Long, HourlyStats>> hourlyStatsStream = keyedByLocation
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(1)))
                .aggregate(new HourlyAggregator(), new HourlyProcessFunction());

        // 写入HBase
        hourlyStatsStream.addSink(new HourlyStatsHBaseSink());

    }

    // 判断客车类型
    private static boolean isPassengerVehicle(int vehicleType) {
        return vehicleType >= 1 && vehicleType <= 4;
    }

    // 判断货车类型
    private static boolean isFreightVehicle(int vehicleType) {
        return vehicleType >= 5 && vehicleType <= 8;
    }

    // 每小时聚合函数
    private static class HourlyAggregator implements AggregateFunction<VehicleEvent, HourlyStats, HourlyStats> {
        @Override
        public HourlyStats createAccumulator() {
            return new HourlyStats();
        }

        @Override
        public HourlyStats add(VehicleEvent event, HourlyStats accumulator) {
            accumulator.addVehicle(event);
            return accumulator;
        }

        @Override
        public HourlyStats getResult(HourlyStats accumulator) {
            return accumulator;
        }

        @Override
        public HourlyStats merge(HourlyStats a, HourlyStats b) {
            a.merge(b);
            return a;
        }
    }

    // 每小时处理函数
    private static class HourlyProcessFunction extends ProcessWindowFunction<
            HourlyStats,
            Tuple3<String, Long, HourlyStats>,
            String,
            TimeWindow> {

        @Override
        public void process(
                String locationId,
                Context context,
                Iterable<HourlyStats> elements,
                Collector<Tuple3<String, Long, HourlyStats>> out) {

            HourlyStats stats = elements.iterator().next();
            long windowStart = context.window().getStart();

            out.collect(Tuple3.of(locationId, windowStart, stats));
        }
    }

    // HBase Sink
    private static class HourlyStatsHBaseSink extends RichSinkFunction<Tuple3<String, Long, HourlyStats>> {
        private transient Connection connection;
        private transient BufferedMutator mutator;
        private transient Map<String, HourlyStats> lastStats = new ConcurrentHashMap<>();
        private transient Timer timer;
        private transient AtomicInteger counter = new AtomicInteger(0);

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142");
            conf.set("hbase.zookeeper.property.clientPort", "2181");

            connection = ConnectionFactory.createConnection(conf);
            TableName tableName = TableName.valueOf(TABLE_NAME);

            // 创建表（如果不存在）
            try (Admin admin = connection.getAdmin()) {
                if (!admin.tableExists(tableName)) {
                    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                    tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
                    admin.createTable(tableDescriptor);
                    System.out.println("表创建成功: " + TABLE_NAME);
                }
            }

            BufferedMutatorParams params = new BufferedMutatorParams(tableName)
                    .writeBufferSize(2 * 1024 * 1024); // 2MB缓冲区
            mutator = connection.getBufferedMutator(params);

            // 每分钟刷新一次
            timer = new Timer(true);
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        flushToHBase();
                    } catch (Exception e) {
                        System.err.println("定时刷新失败: " + e.getMessage());
                    }
                }
            }, 60_000, 60_000); // 每分钟执行一次
        }

        @Override
        public void invoke(Tuple3<String, Long, HourlyStats> value, Context context) throws Exception {
            String locationId = value.f0;
            long hourStart = value.f1;
            HourlyStats stats = value.f2;

            // 更新最新统计数据
            String key = locationId + "_" + hourStart;
            lastStats.put(key, stats);

            // 每100条记录刷新一次
            if (counter.incrementAndGet() % 100 == 0) {
                flushToHBase();
            }
        }

        private void flushToHBase() throws IOException {
            if (lastStats.isEmpty()) return;

            System.out.println("刷新HBase数据，更新" + lastStats.size() + "条记录");

            List<Put> puts = new ArrayList<>();
            for (Map.Entry<String, HourlyStats> entry : lastStats.entrySet()) {
                String[] parts = entry.getKey().split("_");
                String locationId = parts[0];
                long hourStart = Long.parseLong(parts[1]);
                HourlyStats stats = entry.getValue();

                String rowKey = locationId + "_" + hourStart;
                Put put = new Put(Bytes.toBytes(rowKey));

                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("total_vehicles"),
                        Bytes.toBytes(String.valueOf(stats.getTotalVehicles())));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("passenger_vehicles"),
                        Bytes.toBytes(String.valueOf(stats.getPassengerVehicles())));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("freight_vehicles"),
                        Bytes.toBytes(String.valueOf(stats.getFreightVehicles())));

                double avgSpeed = Math.round(stats.getAverageSpeed() * 100.0) / 100.0;
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("avg_speed"),
                        Bytes.toBytes(String.valueOf(avgSpeed)));

                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("last_update"),
                        Bytes.toBytes(String.valueOf(stats.getLastUpdateTime())));

                puts.add(put);
            }

            mutator.mutate(puts);
            mutator.flush();
            lastStats.clear();
        }

        @Override
        public void close() throws Exception {
            if (timer != null) {
                timer.cancel();
            }

            // 关闭前刷新所有数据
            flushToHBase();

            if (mutator != null) {
                mutator.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}