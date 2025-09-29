package whu.edu.moniData.ingest.holyAnalysisJob.useFul;

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
import org.apache.flink.api.java.tuple.Tuple6;
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
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
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

public class numIngestMinute {

    private static final DateTimeFormatter JSON_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
    private static final String tableName="ramp_minute_traffic"; // 修改表名
    private static final DateTimeFormatter MINUTE_ROWKEY_FORMATTER = // 修改为分钟格式
            DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

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

                                    out.collect(new RampVehicleEvent(
                                            vehicleId,
                                            eventTimestamp,
                                            rampId,
                                            vehicleClass,
                                            speed
                                    ));

                                    System.out.println("vehicleId"+vehicleId+" eventTimestamp"+eventTimestamp+" rampid"+rampId+" vehicleClass"+vehicleClass+" speed"+speed);
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
                        if ((originalType >= 1 && originalType <= 4) || originalType == 7 || (originalType >= 12 && originalType <= 16)) {
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
                        WatermarkStrategy.<RampVehicleEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner((event, ts) -> event.timestamp)
                );

        // 2. 按车辆ID和匝道编号分组，处理重复数据
        KeyedStream<RampVehicleEvent, Tuple2<Long, String>> keyedByVehicleAndRamp = rampVehicleEvents
                .keyBy(new KeySelector<RampVehicleEvent, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(RampVehicleEvent event) {
                        return Tuple2.of(event.vehicleId, event.rampId);
                    }
                });

        // 3. 处理车辆轨迹 - 确保每辆车在每个匝道每分钟只计数一次
        DataStream<RampVehicleEvent> deduplicatedEvents = keyedByVehicleAndRamp
                .process(new RampVehicleDeduplicationProcess());

        // 4. 转换为处理格式
        DataStream<Tuple5<Long, String, Integer, Integer, Double>> rampTrafficEvents = deduplicatedEvents
                .map(new MapFunction<RampVehicleEvent, Tuple5<Long, String, Integer, Integer, Double>>() {
                    @Override
                    public Tuple5<Long, String, Integer, Integer, Double> map(RampVehicleEvent event) {
//                        System.out.println("处理去重后车辆: ID=" + event.vehicleId +
//                                ", 匝道=" + event.rampId);
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

        // 5. 分钟级统计 (修改为分钟窗口)
        DataStream<Tuple6<String, Long, Long, Long, Long, Double>> minuteRampTraffic = rampTrafficEvents
                .keyBy(new KeySelector<Tuple5<Long, String, Integer, Integer, Double>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(Tuple5<Long, String, Integer, Integer, Double> value) {
                        Instant instant = Instant.ofEpochMilli(value.f0);
                        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());

                        // 按分钟截断时间戳 (修改为分钟截断)
                        ZonedDateTime minuteTruncated = zdt.withSecond(0).withNano(0);
                        long minuteTimestamp = minuteTruncated.toInstant().toEpochMilli();

                        System.out.println("为车辆分配时间窗口: " +
                                LocalDateTime.ofInstant(Instant.ofEpochMilli(minuteTimestamp), ZoneId.systemDefault()));

                        return Tuple2.of(minuteTimestamp, value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1))) // 修改为分钟窗口
                .aggregate(new RampTrafficAggregator(), new RampTrafficResultFunction());

        // 6. 写入HBase
        minuteRampTraffic.addSink(new RampTrafficHBaseSink(tableName, "cf")).name("ramp num Hbase Sink")
                .setParallelism(1);

        env.execute("Ramp Traffic Statistics Job (Minute Level)");
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

    // 匝道车辆去重处理函数
    public static class RampVehicleDeduplicationProcess
            extends KeyedProcessFunction<Tuple2<Long, String>, RampVehicleEvent, RampVehicleEvent> {

        // 存储车辆最近报告的匝道时间
        private transient MapState<Long, Long> lastRampTimeState;

        @Override
        public void open(Configuration parameters) {
            // 状态TTL配置 - 保留2小时 (可以根据需要调整)
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            MapStateDescriptor<Long, Long> descriptor = new MapStateDescriptor<>(
                    "lastRampTimeState",
                    TypeInformation.of(Long.class),
                    TypeInformation.of(Long.class)
            );
            descriptor.enableTimeToLive(ttlConfig);

            lastRampTimeState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(
                RampVehicleEvent event,
                Context ctx,
                Collector<RampVehicleEvent> out) throws Exception {

            // 检查车辆是否在同一分钟内报告过 (修改为分钟检查)
            Long lastTime = lastRampTimeState.get(event.vehicleId);
            long currentMinute = Instant.ofEpochMilli(event.timestamp)
                    .atZone(ZoneId.systemDefault())
                    .withSecond(0).withNano(0) // 修改为分钟截断
                    .toInstant().toEpochMilli();

            if (lastTime == null || lastTime < currentMinute) {
                // 首次在该分钟看到该车辆
                lastRampTimeState.put(event.vehicleId, currentMinute);
//                System.out.println("处理新车辆: ID=" + event.vehicleId +
//                        ", 时间=" + LocalDateTime.ofInstant(Instant.ofEpochMilli(event.timestamp), ZoneId.systemDefault()));
                out.collect(event);
            } else {

            }
            // 否则忽略重复计数
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

    // 匝道交通HBase Sink - 修复版本
    public static class RampTrafficHBaseSink extends RichSinkFunction<Tuple6<String, Long, Long, Long, Long, Double>> {
        private final String tableName;
        private final String columnFamily;
        private transient Connection connection;
        private transient BufferedMutator mutator;
        private transient AtomicInteger counter;
        private transient boolean connectionSuccessful;
        private transient Configuration flinkConfig; // 保存Flink配置

        public RampTrafficHBaseSink(String tableName, String columnFamily) {
            this.tableName = tableName;
            this.columnFamily = columnFamily;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.flinkConfig = parameters; // 保存Flink配置
            try {
                org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
                conf.set("hbase.zookeeper.property.clientPort", "2181");
                conf.set("zookeeper.session.timeout", "120000");
                conf.set("hbase.rpc.timeout", "300000");

                // 添加更多HBase配置
                conf.set("hbase.client.retries.number", "3");
                conf.set("hbase.client.pause", "1000");
                conf.set("hbase.client.operation.timeout", "30000");
                conf.set("hbase.client.scanner.timeout.period", "60000");

                System.out.println("尝试连接HBase...");
                connection = ConnectionFactory.createConnection(conf);
                counter = new AtomicInteger(0);

                // 检查表是否存在，如果不存在则创建
                TableName hbaseTable = TableName.valueOf(tableName);
                try (Admin admin = connection.getAdmin()) {
                    if (!admin.tableExists(hbaseTable)) {
                        try {
                            HTableDescriptor desc = new HTableDescriptor(hbaseTable);
                            HColumnDescriptor family = new HColumnDescriptor(columnFamily);
                            family.setMaxVersions(1);
                            desc.addFamily(family);
                            admin.createTable(desc);
                            System.out.println("表创建成功: " + tableName);
                        } catch (Exception e) {
                            System.out.println("表创建失败: " + e.getMessage());
                            // 不抛出异常，继续尝试写入
                        }
                    } else {
                        System.out.println("表已存在: " + tableName);
                    }
                } catch (Exception e) {
                    System.err.println("HBase表检查失败: " + e.getMessage());
                    // 不抛出异常，继续尝试写入
                }

                // 创建BufferedMutator
                BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
                        .writeBufferSize(2 * 1024 * 1024);
                mutator = connection.getBufferedMutator(params);

                connectionSuccessful = true;
                System.out.println("HBase连接成功!");
            } catch (Exception e) {
                System.err.println("HBase连接失败: " + e.getMessage());
                e.printStackTrace();
                connectionSuccessful = false;
                // 不抛出异常，让作业继续运行但不写入HBase
            }
        }

        // 添加重新连接方法
        private void reconnect() {
            try {
                System.out.println("尝试重新连接HBase...");
                closeResources(); // 先关闭现有资源

                // 重新初始化连接
                org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
                conf.set("hbase.zookeeper.property.clientPort", "2181");
                conf.set("zookeeper.session.timeout", "120000");
                conf.set("hbase.rpc.timeout", "300000");

                conf.set("hbase.client.retries.number", "3");
                conf.set("hbase.client.pause", "1000");
                conf.set("hbase.client.operation.timeout", "30000");
                conf.set("hbase.client.scanner.timeout.period", "60000");

                connection = ConnectionFactory.createConnection(conf);

                // 创建BufferedMutator
                BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
                        .writeBufferSize(2 * 1024 * 1024);
                mutator = connection.getBufferedMutator(params);

                connectionSuccessful = true;
                System.out.println("HBase重新连接成功!");
            } catch (Exception e) {
                System.err.println("HBase重新连接失败: " + e.getMessage());
                connectionSuccessful = false;
            }
        }

        // 添加资源关闭方法
        private void closeResources() {
            if (mutator != null) {
                try {
                    mutator.close();
                } catch (Exception e) {
                    System.err.println("关闭mutator失败: " + e.getMessage());
                }
                mutator = null;
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    System.err.println("关闭connection失败: " + e.getMessage());
                }
                connection = null;
            }
        }

        @Override
        public void invoke(Tuple6<String, Long, Long, Long, Long, Double> value, Context context) throws Exception {
            if (!connectionSuccessful) {
                System.err.println("HBase连接未建立，跳过写入");
                return;
            }

            try {
                // 使用分钟格式
                String timeStr = Instant.ofEpochMilli(value.f1)
                        .atZone(ZoneId.systemDefault())
                        .format(MINUTE_ROWKEY_FORMATTER);

                String rowKey = timeStr + "_" + value.f0; // 时间_匝道编号

                // 添加详细输出
                LocalDateTime minuteTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(value.f1), ZoneId.systemDefault());
                System.out.println("===== 准备写入HBase =====");
                System.out.println("时间: " + minuteTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                System.out.println("匝道: " + value.f0);
                System.out.println("总车辆数: " + value.f2);
                System.out.println("客车数: " + value.f3);
                System.out.println("货车数: " + value.f4);
                System.out.println("平均速度: " + value.f5);
                System.out.println("行键: " + rowKey);
                System.out.println("=======================");

                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("total_vehicles"), Bytes.toBytes(String.valueOf(value.f2)));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("bus_count"), Bytes.toBytes(String.valueOf(value.f3)));
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("truck_count"), Bytes.toBytes(String.valueOf(value.f4)));

                String avgSpeed = String.format("%.1f", value.f5);
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("avg_speed"), Bytes.toBytes(avgSpeed));

                System.out.println("执行HBase写入操作...");
                mutator.mutate(put);
                System.out.println("HBase写入操作已提交到缓冲区");

                // 每10条记录刷新一次（更频繁的刷新有助于调试）
                if (counter.incrementAndGet() % 10 == 0) {
                    mutator.flush();
                    System.out.println("HBase缓冲区已刷新，累计写入" + counter.get() + "条记录");
                }

                System.out.println("HBase写入成功! 行键: " + rowKey);
            } catch (Exception e) {
                System.err.println("HBase写入错误: " + e.getMessage());
                e.printStackTrace();

                // 尝试重新连接
                reconnect();
            }
        }

        @Override
        public void close() throws Exception {
            closeResources();
            System.out.println("HBase连接已关闭");
        }
    }
}