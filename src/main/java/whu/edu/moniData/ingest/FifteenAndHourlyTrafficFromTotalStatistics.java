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
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class FifteenAndHourlyTrafficFromTotalStatistics {

    private static final DateTimeFormatter JSON_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    // 时间格式
    private static final DateTimeFormatter MINUTE_ROWKEY_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmm");
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
        String groupId = "fifteen-min-traffic-group";
        String topics = "fiberData1,fiberData2,fiberData3,fiberData4,fiberData5,fiberData6,fiberData7,fiberData8,fiberData9,fiberData10,fiberData11";

        // 使用SimpleStringSchema作为反序列化器
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics.split(" "))
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

        // 1. 解析JSON并提取车辆轨迹数据，使用侧输出流处理错误数据
        SingleOutputStreamOperator<VehicleEvent> vehicleEvents = kafkaStream
                .process(new JsonToVehicleEventProcessFunction());

        // 获取解析错误的数据流
        DataStream<String> parseErrors = vehicleEvents.getSideOutput(PARSE_ERROR_TAG);
        // 可以在这里添加对错误数据的处理，例如写入日志或特定存储
        parseErrors.print("Parse Errors");

        // 分配时间戳和水位线
        DataStream<VehicleEvent> withTimestamps = vehicleEvents
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VehicleEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner((event, ts) -> event.timestamp)
                );

        // 2. 按车辆ID分组，处理重复轨迹
        KeyedStream<VehicleEvent, Long> keyedByVehicle = withTimestamps
                .keyBy(VehicleEvent::getVehicleId);

        // 3. 处理车辆轨迹 - 确保每辆车在每个路段只计数一次
        DataStream<VehicleEvent> deduplicatedEvents = keyedByVehicle
                .process(new VehicleDeduplicationProcess());

        // 4. 转换为处理格式
        DataStream<Tuple5<Long, Integer, Integer, Integer, Double>> trafficEvents = deduplicatedEvents
                .map(new MapFunction<VehicleEvent, Tuple5<Long, Integer, Integer, Integer, Double>>() {
                    @Override
                    public Tuple5<Long, Integer, Integer, Integer, Double> map(VehicleEvent event) {
                        return new Tuple5<>(
                                event.timestamp,
                                event.sectionId,
                                event.direction,
                                event.vehicleClass,
                                event.speed
                        );
                    }
                })
                .returns(Types.TUPLE(
                        Types.LONG,
                        Types.INT,
                        Types.INT,
                        Types.INT,
                        Types.DOUBLE
                ));

        // 5. 15分钟窗口处理
        DataStream<Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double>> trafficFlow = trafficEvents
                .keyBy(new KeySelector<Tuple5<Long, Integer, Integer, Integer, Double>, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> getKey(Tuple5<Long, Integer, Integer, Integer, Double> value) {
                        Instant instant = Instant.ofEpochMilli(value.f0);
                        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());

                        // 按15分钟截断时间戳
                        int minute = zdt.getMinute();
                        int truncatedMinute = (minute / 15) * 15; // 0, 15, 30, 45
                        ZonedDateTime truncated = zdt.withMinute(truncatedMinute).withSecond(0).withNano(0);

                        return Tuple2.of(truncated.toInstant().toEpochMilli(), value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(15)))
                .aggregate(new TrafficAggregator(), new TrafficResultFunction());

        // 6. 写入HBase - 15分钟数据
        trafficFlow.addSink(new TrafficHBaseSink("FifteenMinuteTrafficFlow", "cf", "minute"));

        // 7. 小时级统计
        DataStream<Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double>> hourlyTraffic = trafficEvents
                .keyBy(new KeySelector<Tuple5<Long, Integer, Integer, Integer, Double>, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> getKey(Tuple5<Long, Integer, Integer, Integer, Double> value) {
                        Instant instant = Instant.ofEpochMilli(value.f0);
                        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());

                        // 按小时截断时间戳
                        ZonedDateTime hourlyTruncated = zdt.withMinute(0).withSecond(0).withNano(0);

                        return Tuple2.of(hourlyTruncated.toInstant().toEpochMilli(), value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.hours(1)))
                .aggregate(new TrafficAggregator(), new TrafficResultFunction());

        // 8. 写入HBase - 小时数据
        hourlyTraffic.addSink(new TrafficHBaseSink("HourlyTrafficFlow", "cf", "hour"));

        // 9. 天级统计
        DataStream<Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double>> dailyTraffic = trafficEvents
                .keyBy(new KeySelector<Tuple5<Long, Integer, Integer, Integer, Double>, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> getKey(Tuple5<Long, Integer, Integer, Integer, Double> value) {
                        Instant instant = Instant.ofEpochMilli(value.f0);
                        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());

                        // 按天截断时间戳
                        ZonedDateTime dailyTruncated = zdt.withHour(0).withMinute(0).withSecond(0).withNano(0);

                        return Tuple2.of(dailyTruncated.toInstant().toEpochMilli(), value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.days(1)))
                .aggregate(new TrafficAggregator(), new TrafficResultFunction());

        // 10. 写入HBase - 天数据
        dailyTraffic.addSink(new TrafficHBaseSink("DailyTrafficFlow", "cf", "day"));

        // 11. 月级统计
        DataStream<Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double>> monthlyTraffic = trafficEvents
                .keyBy(new KeySelector<Tuple5<Long, Integer, Integer, Integer, Double>, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> getKey(Tuple5<Long, Integer, Integer, Integer, Double> value) {
                        Instant instant = Instant.ofEpochMilli(value.f0);
                        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());

                        // 按月截断时间戳
                        ZonedDateTime monthlyTruncated = zdt.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);

                        return Tuple2.of(monthlyTruncated.toInstant().toEpochMilli(), value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.days(31)))
                .aggregate(new TrafficAggregator(), new TrafficResultFunction());

        // 12. 写入HBase - 月数据
        monthlyTraffic.addSink(new TrafficHBaseSink("MonthlyTrafficFlow", "cf", "month"));

        env.execute("15-Minute, Hourly, Daily and Monthly Traffic Flow Analysis and Storage Job");
    }

    // JSON解析处理函数，使用侧输出流处理错误
    public static class JsonToVehicleEventProcessFunction
            extends org.apache.flink.streaming.api.functions.ProcessFunction<String, VehicleEvent> {

        @Override
        public void processElement(
                String jsonString,
                org.apache.flink.streaming.api.functions.ProcessFunction<String, VehicleEvent>.Context ctx,
                Collector<VehicleEvent> out) {
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
                    long vehicleId = vehicle.getLong("id"); // 车辆唯一标识
                    String stakeId = vehicle.getString("stakeId");
                    int direction = vehicle.getInt("direction");
                    double speed = vehicle.getDouble("speed");
                    int vehicleType = vehicle.getInt("vehicleType");

                    int sectionId = calculateSectionId(stakeId);
                    int vehicleClass = getVehicleClass(vehicleType);

                    if (vehicleClass != -1 && sectionId != -1) {
                        out.collect(new VehicleEvent(
                                vehicleId,
                                eventTimestamp,
                                sectionId,
                                direction,
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

        private int calculateSectionId(String stakeId) {
            try {
                String cleanStake = stakeId.replace("K", "").replace("+", "");
                double mileage = Double.parseDouble(cleanStake) / 1000.0;
                return (int) Math.floor(mileage / 10);
            } catch (NumberFormatException e) {
                return -1;
            }
        }

        private int getVehicleClass(int vehicleType) {
            if ((vehicleType >= 1 && vehicleType <= 4) || vehicleType == 7 || vehicleType == 15) {
                return 0; // 客车
            }
            if (vehicleType == 8 || vehicleType == 10 || vehicleType == 11 ||
                    (vehicleType >= 170 && vehicleType <= 177)) {
                return 1; // 货车
            }
            return -1;
        }
    }

    // 车辆事件数据结构
    public static class VehicleEvent {
        @Getter
        public final Long vehicleId;
        public final long timestamp;
        public final int sectionId;
        public final int direction;
        public final int vehicleClass;
        public final double speed;

        public VehicleEvent(Long vehicleId, long timestamp, int sectionId, int direction, int vehicleClass, double speed) {
            this.vehicleId = vehicleId;
            this.timestamp = timestamp;
            this.sectionId = sectionId;
            this.direction = direction;
            this.vehicleClass = vehicleClass;
            this.speed = speed;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VehicleEvent that = (VehicleEvent) o;
            return timestamp == that.timestamp &&
                    sectionId == that.sectionId &&
                    direction == that.direction &&
                    vehicleClass == that.vehicleClass &&
                    Double.compare(that.speed, speed) == 0 &&
                    Objects.equals(vehicleId, that.vehicleId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vehicleId, timestamp, sectionId, direction, vehicleClass, speed);
        }
    }

    // 车辆去重处理函数 - 使用ValueState替代MapState解决OOM问题
    public static class VehicleDeduplicationProcess
            extends KeyedProcessFunction<Long, VehicleEvent, VehicleEvent> {

        // 存储车辆的上一个事件信息
        private transient ValueState<LastEventInfo> lastEventState;

        // 存储上一个事件的信息
        public static class LastEventInfo {
            public long lastWindowStartTime;
            public int lastSectionId;

            public LastEventInfo() {}

            public LastEventInfo(long lastWindowStartTime, int lastSectionId) {
                this.lastWindowStartTime = lastWindowStartTime;
                this.lastSectionId = lastSectionId;
            }
        }

        @Override
        public void open(Configuration parameters) {
            // 状态TTL配置 - 保留1小时（覆盖4个15分钟窗口）
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<LastEventInfo> descriptor = new ValueStateDescriptor<>(
                    "lastEventState",
                    TypeInformation.of(LastEventInfo.class)
            );
            descriptor.enableTimeToLive(ttlConfig);

            lastEventState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                VehicleEvent event,
                Context ctx,
                Collector<VehicleEvent> out) throws Exception {

            // 按15分钟截断时间戳
            ZonedDateTime zdt = Instant.ofEpochMilli(event.timestamp)
                    .atZone(ZoneId.systemDefault());
            int minute = zdt.getMinute();
            int truncatedMinute = (minute / 15) * 15; // 0, 15, 30, 45
            ZonedDateTime truncated = zdt.withMinute(truncatedMinute).withSecond(0).withNano(0);
            long currentWindowTimestamp = truncated.toInstant().toEpochMilli();

            // 获取上一次事件的信息
            LastEventInfo lastInfo = lastEventState.value();

            if (lastInfo == null) {
                // 第一次见到这辆车，记录信息并发射事件
                lastEventState.update(new LastEventInfo(currentWindowTimestamp, event.sectionId));
                out.collect(event);
            } else {
                // 检查是否在同一窗口
                if (lastInfo.lastWindowStartTime != currentWindowTimestamp) {
                    // 新窗口，记录信息并发射事件
                    lastInfo.lastWindowStartTime = currentWindowTimestamp;
                    lastInfo.lastSectionId = event.sectionId;
                    lastEventState.update(lastInfo);
                    out.collect(event);
                } else {
                    // 同一窗口，检查是否同一路段
                    if (lastInfo.lastSectionId != event.sectionId) {
                        // 不同路段，更新信息并发射事件
                        lastInfo.lastSectionId = event.sectionId;
                        lastEventState.update(lastInfo);
                        out.collect(event);
                    }
                    // 否则（同一窗口同一路段），忽略重复报告
                }
            }
        }
    }

    // 自定义聚合函数
    public static class TrafficAggregator implements org.apache.flink.api.common.functions.AggregateFunction<
            Tuple5<Long, Integer, Integer, Integer, Double>,
            Tuple7<Long, Long, Long, Long, Double, Double, Integer>,
            Tuple7<Long, Long, Long, Long, Double, Double, Integer>> {

        @Override
        public Tuple7<Long, Long, Long, Long, Double, Double, Integer> createAccumulator() {
            return new Tuple7<>(0L, 0L, 0L, 0L, 0.0, 0.0, 0);
        }

        @Override
        public Tuple7<Long, Long, Long, Long, Double, Double, Integer> add(
                Tuple5<Long, Integer, Integer, Integer, Double> value,
                Tuple7<Long, Long, Long, Long, Double, Double, Integer> accumulator) {

            int direction = value.f2;
            int vehicleClass = value.f3;
            double speed = value.f4;

            if (direction == 1) { // 上行
                if (vehicleClass == 0) {
                    accumulator.f0++; // 上行客车
                } else {
                    accumulator.f1++; // 上行货车
                }
                accumulator.f4 += speed;
            } else if (direction == 2) { // 下行
                if (vehicleClass == 0) {
                    accumulator.f2++; // 下行客车
                } else {
                    accumulator.f3++; // 下行货车
                }
                accumulator.f5 += speed;
            }

            accumulator.f6++; // 总计数增加
            return accumulator;
        }

        @Override
        public Tuple7<Long, Long, Long, Long, Double, Double, Integer> getResult(
                Tuple7<Long, Long, Long, Long, Double, Double, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple7<Long, Long, Long, Long, Double, Double, Integer> merge(
                Tuple7<Long, Long, Long, Long, Double, Double, Integer> a,
                Tuple7<Long, Long, Long, Long, Double, Double, Integer> b) {
            return new Tuple7<>(
                    a.f0 + b.f0,
                    a.f1 + b.f1,
                    a.f2 + b.f2,
                    a.f3 + b.f3,
                    a.f4 + b.f4,
                    a.f5 + b.f5,
                    a.f6 + b.f6
            );
        }
    }

    // 窗口结果处理函数
    public static class TrafficResultFunction extends ProcessWindowFunction<
            Tuple7<Long, Long, Long, Long, Double, Double, Integer>,
            Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double>,
            Tuple2<Long, Integer>,
            TimeWindow> {

        @Override
        public void process(
                Tuple2<Long, Integer> key,
                Context context,
                Iterable<Tuple7<Long, Long, Long, Long, Double, Double, Integer>> elements,
                Collector<Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double>> out) {

            Tuple7<Long, Long, Long, Long, Double, Double, Integer> result = elements.iterator().next();

            int upVehicleCount = result.f0.intValue() + result.f1.intValue();
            double upAvgSpeed = upVehicleCount > 0 ? result.f4 / upVehicleCount : 0.0;

            int downVehicleCount = result.f2.intValue() + result.f3.intValue();
            double downAvgSpeed = downVehicleCount > 0 ? result.f5 / downVehicleCount : 0.0;

            out.collect(new Tuple8<>(
                    key.f0,
                    key.f1,
                    result.f0,
                    result.f1,
                    result.f2,
                    result.f3,
                    upAvgSpeed,
                    downAvgSpeed
            ));
        }
    }

    // 通用HBase Sink - 支持不同时间粒度
    public static class TrafficHBaseSink extends RichSinkFunction<Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double>> {
        private final String tableName;
        private final String columnFamily;
        private final String timeGranularity;
        private transient Connection connection;
        private transient BufferedMutator mutator;
        private transient AtomicInteger counter;

        public TrafficHBaseSink(String tableName, String columnFamily, String timeGranularity) {
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
        public void invoke(Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double> value, Context context) throws Exception {
            String timeStr;
            switch (timeGranularity) {
                case "minute":
                    timeStr = Instant.ofEpochMilli(value.f0)
                            .atZone(ZoneId.systemDefault())
                            .format(MINUTE_ROWKEY_FORMATTER);
                    break;
                case "hour":
                    timeStr = Instant.ofEpochMilli(value.f0)
                            .atZone(ZoneId.systemDefault())
                            .format(HOURLY_ROWKEY_FORMATTER);
                    break;
                case "day":
                    timeStr = Instant.ofEpochMilli(value.f0)
                            .atZone(ZoneId.systemDefault())
                            .format(DAILY_ROWKEY_FORMATTER);
                    break;
                case "month":
                    timeStr = Instant.ofEpochMilli(value.f0)
                            .atZone(ZoneId.systemDefault())
                            .format(MONTHLY_ROWKEY_FORMATTER);
                    break;
                default:
                    timeStr = String.valueOf(value.f0);
            }

            String rowKey = timeStr + "-" + value.f1;
            Put put = new Put(Bytes.toBytes(rowKey));

            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("up_bus"), Bytes.toBytes(String.valueOf(value.f2)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("up_truck"), Bytes.toBytes(String.valueOf(value.f3)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("down_bus"), Bytes.toBytes(String.valueOf(value.f4)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("down_truck"), Bytes.toBytes(String.valueOf(value.f5)));

            String upAvgSpeed = String.format("%.1f", value.f6);
            String downAvgSpeed = String.format("%.1f", value.f7);
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("up_avg_speed"), Bytes.toBytes(upAvgSpeed));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("down_avg_speed"), Bytes.toBytes(downAvgSpeed));

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