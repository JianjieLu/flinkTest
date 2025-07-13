package whu.edu.moniData.ingest;

import lombok.Getter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
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

public class HourlyTrafficFromTotalStatistics {

    private static final DateTimeFormatter JSON_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    private static final DateTimeFormatter ROWKEY_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHH");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String brokers = "100.65.38.40:9092";
        String groupId = "hourly-traffic-group";
        String topics = "fiberData1 fiberData2 fiberData3 fiberData4 fiberData5 fiberData6 fiberData7 fiberData8 fiberData9 fiberData10 fiberData11";

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

        // 1. 解析JSON并提取车辆轨迹数据
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
                        } catch (JSONException e) {
                            System.err.println("JSON解析错误: " + e.getMessage() + "\n原始数据: " + jsonString);
                        }
                    }

                    private int calculateSectionId(String stakeId) {
                        try {
                            String cleanStake = stakeId.replace("K", "").replace("+", "");
                            double mileage = Double.parseDouble(cleanStake) / 1000.0;
                            return (int) Math.floor(mileage / 10);
                        } catch (NumberFormatException e) {
                            System.err.println("无效的桩号: " + stakeId);
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
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VehicleEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner((event, ts) -> event.timestamp)
                );

        // 2. 按车辆ID分组，处理重复轨迹
        KeyedStream<VehicleEvent, Long> keyedByVehicle = vehicleEvents
                .keyBy(VehicleEvent::getVehicleId);

        // 3. 处理车辆轨迹 - 确保每辆车在每个路段只计数一次
        DataStream<VehicleEvent> deduplicatedEvents = keyedByVehicle
                .process(new VehicleDeduplicationProcess());

        // 4. 修复：显式指定返回类型
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

        // 5. 窗口处理
        DataStream<Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double>> trafficFlow = trafficEvents
                .keyBy(new KeySelector<Tuple5<Long, Integer, Integer, Integer, Double>, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> getKey(Tuple5<Long, Integer, Integer, Integer, Double> value) {
                        Instant instant = Instant.ofEpochMilli(value.f0);
                        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());
                        long hourTimestamp = zdt.truncatedTo(ChronoUnit.HOURS).toInstant().toEpochMilli();
                        return Tuple2.of(hourTimestamp, value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.hours(1)))
                .aggregate(new TrafficAggregator(), new TrafficResultFunction());

        // 6. 写入HBase
        trafficFlow.addSink(new HourlyTrafficHBaseSink("HourlyTrafficFlow", "cf"));

        env.execute("Hourly Traffic Flow Analysis and Storage Job");
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

    // 车辆去重处理函数
    public static class VehicleDeduplicationProcess
            extends KeyedProcessFunction<Long, VehicleEvent, VehicleEvent> {

        // 存储车辆最近报告的路段
        private transient MapState<Long, Integer> lastSectionState;

        @Override
        public void open(Configuration parameters) {
            // 状态TTL配置 - 保留1小时
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            MapStateDescriptor<Long, Integer> descriptor = new MapStateDescriptor<>(
                    "lastSectionState",
                    TypeInformation.of(Long.class),
                    TypeInformation.of(Integer.class)
            );
            descriptor.enableTimeToLive(ttlConfig);

            lastSectionState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(
                VehicleEvent event,
                Context ctx,
                Collector<VehicleEvent> out) throws Exception {

            // 获取小时时间戳
            long hourTimestamp = Instant.ofEpochMilli(event.timestamp)
                    .atZone(ZoneId.systemDefault())
                    .truncatedTo(ChronoUnit.HOURS)
                    .toInstant()
                    .toEpochMilli();

            // 检查是否在同一小时同一路段报告过
            Integer lastSection = lastSectionState.get(hourTimestamp);
            if (lastSection == null || lastSection != event.sectionId) {
                // 首次在该小时该路段报告
                lastSectionState.put(hourTimestamp, event.sectionId);
                out.collect(event);
            }
            // 否则忽略重复报告
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

    // HBase Sink
    public static class HourlyTrafficHBaseSink extends RichSinkFunction<Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double>> {
        private final String tableName;
        private final String columnFamily;
        private transient Connection connection;
        private transient BufferedMutator mutator;
        private transient AtomicInteger counter;

        public HourlyTrafficHBaseSink(String tableName, String columnFamily) {
            this.tableName = tableName;
            this.columnFamily = columnFamily;
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
            String hourStr = Instant.ofEpochMilli(value.f0)
                    .atZone(ZoneId.systemDefault())
                    .format(ROWKEY_FORMATTER);

            String rowKey = hourStr + "-" + value.f1;
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