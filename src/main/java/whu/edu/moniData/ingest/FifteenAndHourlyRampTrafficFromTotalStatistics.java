package whu.edu.moniData.ingest;

import lombok.Getter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class FifteenAndHourlyRampTrafficFromTotalStatistics {

    private static final DateTimeFormatter JSON_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    private static final DateTimeFormatter HOURLY_ROWKEY_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHH");

    private static final DateTimeFormatter DAILY_ROWKEY_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd");

    private static final DateTimeFormatter MONTHLY_ROWKEY_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMM");

    // 定义侧输出流标签用于处理解析错误的数据
    private static final OutputTag<String> PARSE_ERROR_TAG = new OutputTag<String>("parse-errors") {};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 设置RocksDB状态后端以防止内存溢出
        // env.setStateBackend(new RocksDBStateBackend("file:///tmp/checkpoints", true));

        String brokers = "10.48.53.82:9092";
        String groupId = "ramp-traffic-group";
        String topic = "MergedPathData";

        // 使用SimpleStringSchema作为反序列化器
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // 使用事件时间的水位线策略
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

        // 1. 解析JSON并提取匝道车辆数据，使用侧输出流处理错误数据
        SingleOutputStreamOperator<RampVehicleEvent> rampVehicleEvents = kafkaStream
                .process(new JsonToRampVehicleEventProcessFunction());

        // 获取解析错误的数据流
        DataStream<String> parseErrors = rampVehicleEvents.getSideOutput(PARSE_ERROR_TAG);
        // 可以在这里添加对错误数据的处理，例如写入日志或特定存储
        parseErrors.print("Parse Errors");

        // 分配时间戳和水位线
        DataStream<RampVehicleEvent> withTimestamps = rampVehicleEvents
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<RampVehicleEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner((event, ts) -> event.timestamp)
                );

        // 2. 按车辆ID和匝道编号分组，处理重复数据
        KeyedStream<RampVehicleEvent, Tuple2<Long, String>> keyedByVehicleAndRamp = withTimestamps
                .keyBy(new KeySelector<RampVehicleEvent, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(RampVehicleEvent event) {
                        return Tuple2.of(event.vehicleId, event.rampId);
                    }
                });

        // 3. 处理车辆轨迹 - 确保每辆车在每个匝道只计数一次
        DataStream<RampVehicleEvent> deduplicatedEvents = keyedByVehicleAndRamp
                .process(new RampVehicleDeduplicationProcess());

        // 4. 转换为处理格式
        DataStream<Tuple5<Long, String, Integer, Integer, Double>> rampTrafficEvents = deduplicatedEvents
                .map(new MapFunction<RampVehicleEvent, Tuple5<Long, String, Integer, Integer, Double>>() {
                    @Override
                    public Tuple5<Long, String, Integer, Integer, Double> map(RampVehicleEvent event) {
                        return new Tuple5<>(
                                event.timestamp,
                                event.rampId,
                                1, // 车辆计数
                                event.vehicleClass == 0 ? 1 : 0, // 客车计数
                                event.speed // 速度
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
                        ZonedDateTime hourlyTruncated = zdt.withMinute(0).withSecond(0).withNano(0);

                        return Tuple2.of(hourlyTruncated.toInstant().toEpochMilli(), value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.hours(1)))
                .aggregate(new RampTrafficAggregator(), new RampTrafficResultFunction());

        // 6. 天级统计
        DataStream<Tuple6<String, Long, Long, Long, Long, Double>> dailyRampTraffic = rampTrafficEvents
                .keyBy(new KeySelector<Tuple5<Long, String, Integer, Integer, Double>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(Tuple5<Long, String, Integer, Integer, Double> value) {
                        Instant instant = Instant.ofEpochMilli(value.f0);
                        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());
                        ZonedDateTime dailyTruncated = zdt.withHour(0).withMinute(0).withSecond(0).withNano(0);

                        return Tuple2.of(dailyTruncated.toInstant().toEpochMilli(), value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.days(1)))
                .aggregate(new RampTrafficAggregator(), new RampTrafficResultFunction());

        // 7. 月级统计
        DataStream<Tuple6<String, Long, Long, Long, Long, Double>> monthlyRampTraffic = rampTrafficEvents
                .keyBy(new KeySelector<Tuple5<Long, String, Integer, Integer, Double>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(Tuple5<Long, String, Integer, Integer, Double> value) {
                        Instant instant = Instant.ofEpochMilli(value.f0);
                        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());
                        ZonedDateTime monthlyTruncated = zdt.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);

                        return Tuple2.of(monthlyTruncated.toInstant().toEpochMilli(), value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.days(31)))
                .aggregate(new RampTrafficAggregator(), new RampTrafficResultFunction());

        // 8. 写入HBase - 小时数据
        hourlyRampTraffic.addSink(new RampTrafficHBaseSink("ramp_hour_traffic", "cf", "hourly"));

        // 9. 写入HBase - 天数据
        dailyRampTraffic.addSink(new RampTrafficHBaseSink("ramp_day_traffic", "cf", "daily"));

        // 10. 写入HBase - 月数据
        monthlyRampTraffic.addSink(new RampTrafficHBaseSink("ramp_month_traffic", "cf", "monthly"));

        env.execute("Ramp Traffic Statistics Job");
    }

    // JSON解析处理函数，使用侧输出流处理错误
    public static class JsonToRampVehicleEventProcessFunction
            extends org.apache.flink.streaming.api.functions.ProcessFunction<String, RampVehicleEvent> {

        @Override
        public void processElement(
                String jsonString,
                org.apache.flink.streaming.api.functions.ProcessFunction<String, RampVehicleEvent>.Context ctx,
                Collector<RampVehicleEvent> out) {
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
                        out.collect(new RampVehicleEvent(
                                vehicleId,
                                eventTimestamp,
                                rampId,
                                vehicleClass,
                                speed
                        ));
                    }
                }
            } catch (Exception e) {
                // 将解析错误的数据输出到侧输出流
                ctx.output(PARSE_ERROR_TAG, "JSON解析错误: " + e.getMessage() + "\n原始数据: " + jsonString);
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
                return null;
            }
        }

        private int getVehicleClass(int originalType) {
            if ((originalType >= 1 && originalType <= 4) || originalType == 7 || originalType == 15) {
                return 0; // 客车
            }
            if (originalType == 8 || originalType == 10 || originalType == 11 ||
                    (originalType >= 170 && originalType <= 177)) {
                return 1; // 货车
            }
            return -1;
        }
    }

    // 匝道车辆事件数据结构
    public static class RampVehicleEvent {
        @Getter
        public final Long vehicleId;
        public final long timestamp;
        public final String rampId;
        public final int vehicleClass;
        public final double speed;

        public RampVehicleEvent(Long vehicleId, long timestamp, String rampId, int vehicleClass, double speed) {
            this.vehicleId = vehicleId;
            this.timestamp = timestamp;
            this.rampId = rampId;
            this.vehicleClass = vehicleClass;
            this.speed = speed;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RampVehicleEvent that = (RampVehicleEvent) o;
            return timestamp == that.timestamp &&
                    vehicleClass == that.vehicleClass &&
                    Double.compare(that.speed, speed) == 0 &&
                    Objects.equals(vehicleId, that.vehicleId) &&
                    Objects.equals(rampId, that.rampId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vehicleId, timestamp, rampId, vehicleClass, speed);
        }
    }

    // 匝道车辆去重处理函数 - 使用ValueState替代MapState解决OOM问题
    public static class RampVehicleDeduplicationProcess
            extends KeyedProcessFunction<Tuple2<Long, String>, RampVehicleEvent, RampVehicleEvent> {

        // 存储车辆最近报告的匝道时间
        private transient ValueState<Long> lastRampTimeState;

        @Override
        public void open(Configuration parameters) {
            // 状态TTL配置 - 保留24小时
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                    "lastRampTimeState",
                    TypeInformation.of(Long.class)
            );
            descriptor.enableTimeToLive(ttlConfig);

            lastRampTimeState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                RampVehicleEvent event,
                Context ctx,
                Collector<RampVehicleEvent> out) throws Exception {

            // 检查车辆是否在同一小时内报告过
            Long lastTime = lastRampTimeState.value();
            long currentHour = Instant.ofEpochMilli(event.timestamp)
                    .atZone(ZoneId.systemDefault())
                    .withMinute(0).withSecond(0).withNano(0)
                    .toInstant().toEpochMilli();

            if (lastTime == null || lastTime < currentHour) {
                // 首次在该小时报告
                lastRampTimeState.update(currentHour);
                out.collect(event);
            }
            // 否则忽略重复报告
        }
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
        private final String tableName;
        private final String columnFamily;
        private final String timeGranularity;
        private transient Connection connection;
        private transient BufferedMutator mutator;
        private transient AtomicInteger counter;

        public RampTrafficHBaseSink(String tableName, String columnFamily, String timeGranularity) {
            this.tableName = tableName;
            this.columnFamily = columnFamily;
            this.timeGranularity = timeGranularity;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.client.write.buffer", "2097152");
            conf.set("hbase.rpc.timeout", "60000");
            conf.set("hbase.client.operation.timeout", "60000");

            connection = ConnectionFactory.createConnection(conf);
            counter = new AtomicInteger(0);

            TableName hbaseTable = TableName.valueOf(tableName);
            try (Admin admin = connection.getAdmin()) {
                if (!admin.tableExists(hbaseTable)) {
                    try {
                        HTableDescriptor desc = new HTableDescriptor(hbaseTable);
                        desc.addFamily(new HColumnDescriptor(columnFamily));
                        admin.createTable(desc);
                        System.out.println("表创建成功: " + tableName);
                    } catch (Exception e) {
                        System.out.println("表创建失败或已存在: " + e.getMessage());
                    }
                } else {
                    System.out.println("表已存在: " + tableName);
                }
            } catch (Exception e) {
                System.err.println("HBase表检查失败: " + e.getMessage());
            }

            BufferedMutatorParams params = new BufferedMutatorParams(hbaseTable)
                    .writeBufferSize(2 * 1024 * 1024);
            mutator = connection.getBufferedMutator(params);
        }

        @Override
        public void invoke(Tuple6<String, Long, Long, Long, Long, Double> value, Context context) throws Exception {
            String timeStr;
            switch (timeGranularity) {
                case "hourly":
                    timeStr = Instant.ofEpochMilli(value.f1)
                            .atZone(ZoneId.systemDefault())
                            .format(HOURLY_ROWKEY_FORMATTER);
                    break;
                case "daily":
                    timeStr = Instant.ofEpochMilli(value.f1)
                            .atZone(ZoneId.systemDefault())
                            .format(DAILY_ROWKEY_FORMATTER);
                    break;
                case "monthly":
                    timeStr = Instant.ofEpochMilli(value.f1)
                            .atZone(ZoneId.systemDefault())
                            .format(MONTHLY_ROWKEY_FORMATTER);
                    break;
                default:
                    timeStr = String.valueOf(value.f1);
            }

            String rowKey = timeStr + "-" + value.f0; // 时间-匝道编号
            Put put = new Put(Bytes.toBytes(rowKey));

            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("total_vehicles"), Bytes.toBytes(String.valueOf(value.f2)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bus_count"), Bytes.toBytes(String.valueOf(value.f3)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("truck_count"), Bytes.toBytes(String.valueOf(value.f4)));

            String avgSpeed = String.format("%.1f", value.f5);
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("avg_speed"), Bytes.toBytes(avgSpeed));

            mutator.mutate(put);

            // 每100条记录刷新一次
            if (counter.incrementAndGet() % 100 == 0) {
                mutator.flush();
            }
        }

        @Override
        public void close() throws Exception {
            if (mutator != null) {
                try {
                    mutator.flush();
                } catch (Exception e) {
                    System.err.println("HBase刷新失败: " + e.getMessage());
                }
                mutator.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }
}