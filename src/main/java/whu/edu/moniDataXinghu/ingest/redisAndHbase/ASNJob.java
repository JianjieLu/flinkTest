package whu.edu.moniDataXinghu.ingest.redisAndHbase;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

public class ASNJob {

    // 定义时间格式化器
    private static final DateTimeFormatter JSON_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
    private static final DateTimeFormatter HOUR_ROWKEY_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHH");
    private static final String TABLE_NAME = "ramp_hour_traffic";
    private static final String COLUMN_FAMILY = "cf";

    // 匝道车辆事件数据结构
    public static class RampVehicleEvent {
        private Long vehicleId;
        private long timestamp;
        private String rampId;
        private int vehicleClass;
        private double speed;

        public RampVehicleEvent(Long vehicleId, long timestamp, String rampId, int vehicleClass, double speed) {
            this.vehicleId = vehicleId;
            this.timestamp = timestamp;
            this.rampId = rampId;
            this.vehicleClass = vehicleClass;
            this.speed = speed;
        }

        public Long getVehicleId() {
            return vehicleId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getRampId() {
            return rampId;
        }

        public int getVehicleClass() {
            return vehicleClass;
        }

        public double getSpeed() {
            return speed;
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Kafka配置
        String brokers = "192.168.0.5:9092";
        String groupId = "ramp-traffic-group";
        String topic = "MergedRampPathData";

        // 创建Kafka源
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 使用事件时间的水位线策略
        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
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

        // 1. 解析JSON并提取匝道车辆数据
        DataStream<RampVehicleEvent> rampVehicleEvents = kafkaStream
                .flatMap(new FlatMapFunction<String, RampVehicleEvent>() {
                    @Override
                    public void flatMap(String jsonString, Collector<RampVehicleEvent> out) {
                        try {
                            JSONObject jsonObject = new JSONObject(jsonString);
                            String timeStampStr = jsonObject.getString("timeStamp");

                            long eventTimestamp = LocalDateTime.parse(timeStampStr, JSON_TIME_FORMATTER)
                                    .atZone(ZoneId.systemDefault())
                                    .toInstant()
                                    .toEpochMilli();

                            JSONArray pathList = jsonObject.getJSONArray("pathList");
                            for (int i = 0; i < pathList.length(); i++) {
                                JSONObject vehicle = pathList.getJSONObject(i);
                                long vehicleId = vehicle.getLong("id");
                                String stakeId = vehicle.getString("stakeId");
                                double speed = vehicle.getDouble("speed");
                                int originalType = vehicle.getInt("originalType");

                                // 提取匝道编号
                                String rampId = extractRampId(stakeId);
                                int vehicleClass = getVehicleClass(originalType);

                                if (rampId != null && vehicleClass != -1) {
                                    System.out.println("处理车辆: ID=" + vehicleId +
                                            ", 时间=" + timeStampStr +
                                            ", 匝道=" + rampId +
                                            ", 类型=" + vehicleClass +
                                            ", 速度=" + speed);

                                    out.collect(new RampVehicleEvent(
                                            vehicleId,
                                            eventTimestamp,
                                            rampId,
                                            vehicleClass,
                                            speed
                                    ));
                                }
                            }
                        } catch (JSONException e) {
                            System.err.println("JSON解析错误: " + e.getMessage() + "\n原始数据: " + jsonString);
                        }
                    }

                    private String extractRampId(String stakeId) {
                        try {
                            // 从桩号中提取匝道编号 (A, B, C, D)
                            if (stakeId.contains("-A")) return "A";
                            if (stakeId.contains("-B")) return "B";
                            if (stakeId.contains("-C")) return "C";
                            if (stakeId.contains("-D")) return "D";
                            return null;
                        } catch (Exception e) {
                            System.err.println("无效的桩号: " + stakeId);
                            return null;
                        }
                    }

                    private int getVehicleClass(int originalType) {
                        if ((originalType >= 1 && originalType <= 4) || originalType == 7 ||
                                (originalType >= 12 && originalType <= 16)) {
                            return 0; // 客车
                        }
                        if (originalType == 8 || originalType == 10 || originalType == 11 ||
                                (originalType >= 170 && originalType <= 177)) {
                            return 1; // 货车
                        }
                        return -1;
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<RampVehicleEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, ts) -> event.getTimestamp())
                );

        // 2. 按车辆ID和匝道编号分组，处理重复数据
        KeyedStream<RampVehicleEvent, Tuple2<Long, String>> keyedByVehicleAndRamp = rampVehicleEvents
                .keyBy(new KeySelector<RampVehicleEvent, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(RampVehicleEvent event) {
                        return Tuple2.of(event.getVehicleId(), event.getRampId());
                    }
                });

        // 3. 处理车辆轨迹 - 确保每辆车在每个匝道每小时只计数一次
        DataStream<RampVehicleEvent> deduplicatedEvents = keyedByVehicleAndRamp
                .process(new KeyedProcessFunction<Tuple2<Long, String>, RampVehicleEvent, RampVehicleEvent>() {

                    // 存储车辆最近报告的匝道时间
                    private transient MapState<Long, Long> lastRampTimeState;

                    @Override
                    public void open(Configuration parameters) {
                        MapStateDescriptor<Long, Long> descriptor = new MapStateDescriptor<>(
                                "lastRampTimeState",
                                Types.LONG,
                                Types.LONG
                        );
                        lastRampTimeState = getRuntimeContext().getMapState(descriptor);
                    }

                    @Override
                    public void processElement(
                            RampVehicleEvent event,
                            Context ctx,
                            Collector<RampVehicleEvent> out) throws Exception {
                        // 检查车辆是否在同一小时内报告过
                        Long lastTime = lastRampTimeState.get(event.getVehicleId());
                        long currentHour = Instant.ofEpochMilli(event.getTimestamp())
                                .atZone(ZoneId.systemDefault())
                                .withMinute(0).withSecond(0).withNano(0)
                                .toInstant().toEpochMilli();

                        if (lastTime == null || lastTime < currentHour) {
                            // 首次在该小时看到该车辆
                            lastRampTimeState.put(event.getVehicleId(), currentHour);
                            System.out.println("处理新车辆: ID=" + event.getVehicleId() +
                                    ", 时间=" + LocalDateTime.ofInstant(Instant.ofEpochMilli(event.getTimestamp()), ZoneId.systemDefault()));
                            out.collect(event);
                        }
                        // 否则忽略重复计数
                    }
                });

        // 4. 转换为处理格式
        DataStream<Tuple5<Long, String, Integer, Integer, Double>> rampTrafficEvents = deduplicatedEvents
                .map(new MapFunction<RampVehicleEvent, Tuple5<Long, String, Integer, Integer, Double>>() {
                    @Override
                    public Tuple5<Long, String, Integer, Integer, Double> map(RampVehicleEvent event) {
                        System.out.println("处理去重后车辆: ID=" + event.getVehicleId() +
                                ", 匝道=" + event.getRampId());
                        return new Tuple5<>(
                                event.getTimestamp(),
                                event.getRampId(),
                                1, // 车辆计数
                                event.getVehicleClass() == 0 ? 1 : 0, // 客车计数
                                event.getSpeed() // 速度
                        );
                    }
                })
                .returns(Types.TUPLE(
                        Types.LONG,
                        Types.STRING,
                        Types.INT,
                        Types.INT,
                        Types.DOUBLE
                ));

        // 5. 小时级统计
        DataStream<Tuple6<String, Long, Long, Long, Long, Double>> hourlyRampTraffic = rampTrafficEvents
                .keyBy(new KeySelector<Tuple5<Long, String, Integer, Integer, Double>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(Tuple5<Long, String, Integer, Integer, Double> value) {
                        Instant instant = Instant.ofEpochMilli(value.f0);
                        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());

                        // 按小时截断时间戳
                        ZonedDateTime hourTruncated = zdt.withMinute(0).withSecond(0).withNano(0);
                        long hourTimestamp = hourTruncated.toInstant().toEpochMilli();

                        System.out.println("为车辆分配时间窗口: " +
                                LocalDateTime.ofInstant(Instant.ofEpochMilli(hourTimestamp), ZoneId.systemDefault()));

                        return Tuple2.of(hourTimestamp, value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new RampTrafficAggregator(), new RampTrafficResultFunction());

        // 6. 写入HBase
        hourlyRampTraffic.addSink(new RampTrafficHBaseSink()).name("ramp hourly Hbase Sink")
                .setParallelism(1);

        env.execute("Ramp Traffic Hourly Statistics Job");
    }

    // 匝道交通聚合函数
    public static class RampTrafficAggregator implements org.apache.flink.api.common.functions.AggregateFunction<
            Tuple5<Long, String, Integer, Integer, Double>,
            Tuple5<Long, Long, Long, Double, Integer>,
            Tuple5<Long, Long, Long, Double, Integer>> {

        @Override
        public Tuple5<Long, Long, Long, Double, Integer> createAccumulator() {
            return new Tuple5<>(0L, 0L, 0L, 0.0, 0);
        }

        @Override
        public Tuple5<Long, Long, Long, Double, Integer> add(
                Tuple5<Long, String, Integer, Integer, Double> value,
                Tuple5<Long, Long, Long, Double, Integer> accumulator) {

            accumulator.f0 += value.f2; // 总车辆数
            accumulator.f1 += value.f3; // 客车数
            accumulator.f2 = accumulator.f0 - accumulator.f1; // 货车数 = 总车辆数 - 客车数
            accumulator.f3 += value.f4; // 速度总和
            accumulator.f4++; // 计数增加

            System.out.println("聚合车辆数据: 匝道=" + value.f1 +
                    ", 总车辆数=" + accumulator.f0 +
                    ", 客车数=" + accumulator.f1 +
                    ", 货车数=" + accumulator.f2);

            return accumulator;
        }

        @Override
        public Tuple5<Long, Long, Long, Double, Integer> getResult(
                Tuple5<Long, Long, Long, Double, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple5<Long, Long, Long, Double, Integer> merge(
                Tuple5<Long, Long, Long, Double, Integer> a,
                Tuple5<Long, Long, Long, Double, Integer> b) {
            return new Tuple5<>(
                    a.f0 + b.f0,
                    a.f1 + b.f1,
                    a.f2 + b.f2,
                    a.f3 + b.f3,
                    a.f4 + b.f4
            );
        }
    }

    // 匝道交通结果处理函数
    public static class RampTrafficResultFunction extends ProcessWindowFunction<
            Tuple5<Long, Long, Long, Double, Integer>,
            Tuple6<String, Long, Long, Long, Long, Double>,
            Tuple2<Long, String>,
            TimeWindow> {

        @Override
        public void process(
                Tuple2<Long, String> key,
                Context context,
                Iterable<Tuple5<Long, Long, Long, Double, Integer>> elements,
                Collector<Tuple6<String, Long, Long, Long, Long, Double>> out) {

            Tuple5<Long, Long, Long, Double, Integer> result = elements.iterator().next();

            double avgSpeed = result.f4 > 0 ? result.f3 / result.f4 : 0.0;

            LocalDateTime windowTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(key.f0), ZoneId.systemDefault());
            System.out.println("窗口计算结果: 时间=" + windowTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) +
                    ", 匝道=" + key.f1 +
                    ", 总车辆数=" + result.f0 +
                    ", 客车数=" + result.f1 +
                    ", 货车数=" + result.f2 +
                    ", 平均速度=" + avgSpeed);

            out.collect(new Tuple6<>(
                    key.f1, // 匝道编号
                    key.f0, // 时间戳
                    result.f0, // 总车辆数
                    result.f1, // 客车数
                    result.f2, // 货车数
                    avgSpeed // 平均速度
            ));
        }
    }

    // 匝道交通HBase Sink
    public static class RampTrafficHBaseSink extends RichSinkFunction<Tuple6<String, Long, Long, Long, Long, Double>> {
        private transient Connection connection;
        private transient BufferedMutator mutator;
        private transient AtomicInteger counter;

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "192.168.0.5,192.168.0.7,192.168.0.8:,192.168.0.9,192.168.0.11,192.168.0.12");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("zookeeper.session.timeout", "120000");
            conf.set("hbase.rpc.timeout", "300000");
            conf.set("fs.defaultFS", "hdfs://192.168.0.5:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            System.out.println("尝试连接HBase...");
            connection = ConnectionFactory.createConnection(conf);
            counter = new AtomicInteger(0);

            TableName hbaseTable = TableName.valueOf(TABLE_NAME);
            try (Admin admin = connection.getAdmin()) {
                if (!admin.tableExists(hbaseTable)) {
                    try {
                        HTableDescriptor desc = new HTableDescriptor(hbaseTable);
                        desc.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
                        admin.createTable(desc);
                        System.out.println("表创建成功: " + TABLE_NAME);
                    } catch (Exception e) {
                        System.out.println("表创建失败或已存在: " + e.getMessage());
                    }
                } else {
                    System.out.println("表已存在: " + TABLE_NAME);
                }
            } catch (Exception e) {
                System.err.println("HBase表检查失败: " + e.getMessage());
            }

            BufferedMutatorParams params = new BufferedMutatorParams(hbaseTable)
                    .writeBufferSize(2 * 1024 * 1024);
            mutator = connection.getBufferedMutator(params);
            System.out.println("HBase连接成功!");
        }

        @Override
        public void invoke(Tuple6<String, Long, Long, Long, Long, Double> value, Context context) throws Exception {
            // 使用小时格式
            String timeStr = Instant.ofEpochMilli(value.f1)
                    .atZone(ZoneId.systemDefault())
                    .format(HOUR_ROWKEY_FORMATTER);

            String rowKey = timeStr + "_" + value.f0; // 时间_匝道编号

            // 添加详细输出
            LocalDateTime hourTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(value.f1), ZoneId.systemDefault());
            System.out.println("===== 准备写入HBase =====");
            System.out.println("时间: " + hourTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            System.out.println("匝道: " + value.f0);
            System.out.println("总车辆数: " + value.f2);
            System.out.println("客车数: " + value.f3);
            System.out.println("货车数: " + value.f4);
            System.out.println("平均速度: " + value.f5);
            System.out.println("行键: " + rowKey);
            System.out.println("=======================");

            try {
                Put put = new Put(Bytes.toBytes(rowKey));

                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("total_vehicles"), Bytes.toBytes(String.valueOf(value.f2)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("bus_count"), Bytes.toBytes(String.valueOf(value.f3)));
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("track_count"), Bytes.toBytes(String.valueOf(value.f4)));

                String avgSpeed = String.format("%.1f", value.f5);
                put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("avg_speed"), Bytes.toBytes(avgSpeed));

                System.out.println("执行HBase写入操作...");
                mutator.mutate(put);
                System.out.println("HBase写入操作已提交到缓冲区");

                // 立即刷新以确保数据写入
                mutator.flush();
                System.out.println("HBase写入成功! 行键: " + rowKey);

                // 添加成功插入后的输出
                System.out.println("成功插入记录:");
                System.out.println("  时间: " + hourTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                System.out.println("  匝道: " + value.f0);
                System.out.println("  总车辆数: " + value.f2);
                System.out.println("  客车数: " + value.f3);
                System.out.println("  货车数: " + value.f4);
                System.out.println("  平均速度: " + value.f5);
                System.out.println("  行键: " + rowKey);
            } catch (Exception e) {
                System.err.println("HBase写入失败: " + e.getMessage());
                e.printStackTrace();
            }

            // 每100条记录刷新一次
            if (counter.incrementAndGet() % 100 == 0) {
                System.out.println("已刷新HBase缓冲区，写入" + counter.get() + "条记录");
            }
        }

        @Override
        public void close() throws Exception {
            if (mutator != null) {
                try {
                    mutator.flush();
                    System.out.println("关闭前刷新HBase缓冲区");
                } catch (Exception e) {
                    System.err.println("HBase刷新失败: " + e.getMessage());
                }
                mutator.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
                System.out.println("HBase连接已关闭");
            }
        }
    }
}