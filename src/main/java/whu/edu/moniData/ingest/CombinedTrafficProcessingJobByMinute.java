package whu.edu.moniData.ingest;

import lombok.Getter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
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

public class CombinedTrafficProcessingJobByMinute {

    private static final DateTimeFormatter JSON_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");

    // 时间格式
    private static final DateTimeFormatter MINUTE_ROWKEY_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    // 定义侧输出流标签用于处理解析错误的数据
    private static final OutputTag<String> PARSE_ERROR_TAG = new OutputTag<String>("parse-errors") {};

    public static void main(String[] args) throws Exception {
        System.out.println("========== 作业开始初始化 ==========");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        System.out.println("设置并行度为: 2");

        // 设置RocksDB状态后端以防止内存溢出
        // env.setStateBackend(new RocksDBStateBackend("file:///tmp/checkpoints", true));

        String brokers = "10.48.53.82:9092";
        String rampGroupId = "ramp-traffic-group";
        String sectionGroupId = "fifteen-min-traffic-group";
        String rampTopic = "MergedPathData";
        String sectionTopics = "MergedPathData.sceneTest.1 MergedPathData.sceneTest.2 MergedPathData.sceneTest.3 MergedPathData.sceneTest.4 MergedPathData.sceneTest.5 MergedPathData.sceneTest.6 MergedPathData.sceneTest.7 MergedPathData.sceneTest.8 MergedPathData.sceneTest.9 MergedPathData.sceneTest.10 MergedPathData.sceneTest.11";

        System.out.println("创建匝道数据源...");
        // 创建匝道数据源
        KafkaSource<String> rampSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(rampTopic)
                .setGroupId(rampGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        System.out.println("创建路段数据源...");
        // 创建路段数据源
        KafkaSource<String> sectionSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(sectionTopics.split(" "))
                .setGroupId(sectionGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // 使用事件时间的水位线策略处理匝道数据
        DataStream<String> rampKafkaStream = env.fromSource(
                rampSource,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            try {
                                JSONObject json = new JSONObject(event);
                                String timeStampStr = json.getString("timeStamp");
                                LocalDateTime dateTime = LocalDateTime.parse(timeStampStr, JSON_TIME_FORMATTER);
                                return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            } catch (JSONException e) {
                                System.out.println("解析时间戳失败，使用当前时间");
                                return System.currentTimeMillis();
                            }
                        }),
                "Ramp Kafka Source"
        );

        // 使用事件时间的水位线策略处理路段数据
        DataStream<String> sectionKafkaStream = env.fromSource(
                sectionSource,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                        .withTimestampAssigner((event, timestamp) -> {
                            try {
                                JSONObject json = new JSONObject(event);
                                String timeStampStr = json.getString("timeStamp");
                                LocalDateTime dateTime = LocalDateTime.parse(timeStampStr, JSON_TIME_FORMATTER);
                                return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                            } catch (JSONException e) {
                                System.out.println("解析时间戳失败，使用当前时间");
                                return System.currentTimeMillis();
                            }
                        }),
                "Section Kafka Source"
        );

        System.out.println("开始处理匝道交通数据...");
        // 处理匝道交通数据
        processRampTraffic(rampKafkaStream);

        System.out.println("开始处理路段交通数据...");
        // 处理路段交通数据
        processSectionTraffic(sectionKafkaStream);

        System.out.println("提交作业执行...");
        env.execute("Combined Traffic Processing Job");
        System.out.println("作业执行完成");
    }

    // 处理匝道交通数据
    private static void processRampTraffic(DataStream<String> kafkaStream) {
        System.out.println("步骤1: 解析JSON并提取匝道车辆数据");
        // 1. 解析JSON并提取匝道车辆数据，使用侧输出流处理错误数据
        SingleOutputStreamOperator<RampVehicleEvent> rampVehicleEvents = kafkaStream
                .process(new JsonToRampVehicleEventProcessFunction());

        // 获取解析错误的数据流
        DataStream<String> parseErrors = rampVehicleEvents.getSideOutput(PARSE_ERROR_TAG);
        // 可以在这里添加对错误数据的处理，例如写入日志或特定存储
        parseErrors.print("Ramp Parse Errors");

        System.out.println("步骤2: 分配时间戳和水位线");
        // 分配时间戳和水位线
        DataStream<RampVehicleEvent> withTimestamps = rampVehicleEvents
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<RampVehicleEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner((event, ts) -> event.timestamp)
                );

        System.out.println("步骤3: 按车辆ID和匝道编号分组");
        // 2. 按车辆ID和匝道编号分组，处理重复数据
        KeyedStream<RampVehicleEvent, Tuple2<Long, String>> keyedByVehicleAndRamp = withTimestamps
                .keyBy(new KeySelector<RampVehicleEvent, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(RampVehicleEvent event) {
                        return Tuple2.of(event.vehicleId, event.rampId);
                    }
                });

        System.out.println("步骤4: 处理车辆轨迹去重");
        // 3. 处理车辆轨迹 - 确保每辆车在每个匝道只计数一次
        DataStream<RampVehicleEvent> deduplicatedEvents = keyedByVehicleAndRamp
                .process(new RampVehicleDeduplicationProcess());

        System.out.println("步骤5: 转换为处理格式");
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

        System.out.println("步骤6: 分钟级统计");
        // 5. 分钟级统计
        DataStream<Tuple6<String, Long, Long, Long, Long, Double>> minuteRampTraffic = rampTrafficEvents
                .keyBy(new KeySelector<Tuple5<Long, String, Integer, Integer, Double>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> getKey(Tuple5<Long, String, Integer, Integer, Double> value) {
                        Instant instant = Instant.ofEpochMilli(value.f0);
                        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());
                        // 按分钟截断时间戳
                        ZonedDateTime minuteTruncated = zdt.withSecond(0).withNano(0);

                        return Tuple2.of(minuteTruncated.toInstant().toEpochMilli(), value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                .aggregate(new RampTrafficAggregator(), new RampTrafficResultFunction());

        System.out.println("步骤7: 写入HBase - 分钟数据");
        // 6. 写入HBase - 分钟数据
        minuteRampTraffic.addSink(new RampTrafficHBaseSink("ramp_minute_traffic", "cf", "minute"));
    }

    // 处理路段交通数据
    private static void processSectionTraffic(DataStream<String> kafkaStream) {
        System.out.println("步骤1: 解析JSON并提取车辆轨迹数据");
        // 1. 解析JSON并提取车辆轨迹数据，使用侧输出流处理错误数据
        SingleOutputStreamOperator<SectionVehicleEvent> vehicleEvents = kafkaStream
                .process(new JsonToSectionVehicleEventProcessFunction());

        // 获取解析错误的数据流
        DataStream<String> parseErrors = vehicleEvents.getSideOutput(PARSE_ERROR_TAG);
        // 可以在这里添加对错误数据的处理，例如写入日志或特定存储
        parseErrors.print("Section Parse Errors");

        System.out.println("步骤2: 分配时间戳和水位线");
        // 分配时间戳和水位线
        DataStream<SectionVehicleEvent> withTimestamps = vehicleEvents
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SectionVehicleEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner((event, ts) -> event.timestamp)
                );

        System.out.println("步骤3: 按车辆ID分组");
        // 2. 按车辆ID分组，处理重复轨迹
        KeyedStream<SectionVehicleEvent, Long> keyedByVehicle = withTimestamps
                .keyBy(SectionVehicleEvent::getVehicleId);

        System.out.println("步骤4: 处理车辆轨迹去重");
        // 3. 处理车辆轨迹 - 确保每辆车在每个路段只计数一次
        DataStream<SectionVehicleEvent> deduplicatedEvents = keyedByVehicle
                .process(new SectionVehicleDeduplicationProcess());

        System.out.println("步骤5: 转换为处理格式");
        // 4. 转换为处理格式
        DataStream<Tuple5<Long, Integer, Integer, Integer, Double>> trafficEvents = deduplicatedEvents
                .map(new MapFunction<SectionVehicleEvent, Tuple5<Long, Integer, Integer, Integer, Double>>() {
                    @Override
                    public Tuple5<Long, Integer, Integer, Integer, Double> map(SectionVehicleEvent event) {
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

        System.out.println("步骤6: 分钟窗口处理");
        // 5. 分钟窗口处理
        DataStream<Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double>> trafficFlow = trafficEvents
                .keyBy(new KeySelector<Tuple5<Long, Integer, Integer, Integer, Double>, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> getKey(Tuple5<Long, Integer, Integer, Integer, Double> value) {
                        Instant instant = Instant.ofEpochMilli(value.f0);
                        ZonedDateTime zdt = instant.atZone(ZoneId.systemDefault());

                        // 按分钟截断时间戳
                        ZonedDateTime truncated = zdt.withSecond(0).withNano(0);

                        return Tuple2.of(truncated.toInstant().toEpochMilli(), value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                .aggregate(new SectionTrafficAggregator(), new SectionTrafficResultFunction());

        System.out.println("步骤7: 写入HBase - 分钟数据");
        // 6. 写入HBase - 分钟数据
        trafficFlow.addSink(new SectionTrafficHBaseSink("section_minute_traffic", "cf", "minute"));
    }

    // JSON解析处理函数，使用侧输出流处理错误 - 匝道数据
    public static class JsonToRampVehicleEventProcessFunction
            extends org.apache.flink.streaming.api.functions.ProcessFunction<String, RampVehicleEvent> {

        @Override
        public void processElement(
                String jsonString,
                org.apache.flink.streaming.api.functions.ProcessFunction<String, RampVehicleEvent>.Context ctx,
                Collector<RampVehicleEvent> out) {
            try {
                System.out.println("解析JSON数据: " + jsonString.substring(0, Math.min(50, jsonString.length())) + "...");

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
                    } else {
                        System.out.println("无效的匝道事件: stakeId=" + stakeId + ", originalType=" + originalType);
                    }
                }
            } catch (Exception e) {
                System.out.println("JSON解析错误: " + e.getMessage());
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
            if ((originalType >= 1 && originalType <= 4) || originalType == 7 || (originalType >= 12&&originalType<=16)) {
                return 0; // 客车
            }
            if (originalType == 8 || originalType == 10 || originalType == 11 ||
                    (originalType >= 170 && originalType <= 177)) {
                return 1; // 货车
            }
            return -1;
        }
    }

    // JSON解析处理函数，使用侧输出流处理错误 - 路段数据
    public static class JsonToSectionVehicleEventProcessFunction
            extends org.apache.flink.streaming.api.functions.ProcessFunction<String, SectionVehicleEvent> {

        @Override
        public void processElement(
                String jsonString,
                org.apache.flink.streaming.api.functions.ProcessFunction<String, SectionVehicleEvent>.Context ctx,
                Collector<SectionVehicleEvent> out) {
            try {
                System.out.println("解析JSON数据: " + jsonString.substring(0, Math.min(50, jsonString.length())) + "...");

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
                        out.collect(new SectionVehicleEvent(
                                vehicleId,
                                eventTimestamp,
                                sectionId,
                                direction,
                                vehicleClass,
                                speed
                        ));
                    } else {
                        System.out.println("无效的路段事件: stakeId=" + stakeId + ", vehicleType=" + vehicleType);
                    }
                }
            } catch (Exception e) {
                System.out.println("JSON解析错误: " + e.getMessage());
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
            if ((vehicleType >= 1 && vehicleType <= 4) || vehicleType == 7 || (vehicleType >= 12&&vehicleType<=16)) {
                return 0; // 客车
            }
            if (vehicleType == 8 || vehicleType == 10 || vehicleType == 11 ||
                    (vehicleType >= 170 && vehicleType <= 177)) {
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

    // 路段车辆事件数据结构
    public static class SectionVehicleEvent {
        @Getter
        public final Long vehicleId;
        public final long timestamp;
        public final int sectionId;
        public final int direction;
        public final int vehicleClass;
        public final double speed;

        public SectionVehicleEvent(Long vehicleId, long timestamp, int sectionId, int direction, int vehicleClass, double speed) {
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
            SectionVehicleEvent that = (SectionVehicleEvent) o;
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

    // 匝道车辆去重处理函数 - 使用ValueState替代MapState解决OOM问题
    public static class RampVehicleDeduplicationProcess
            extends KeyedProcessFunction<Tuple2<Long, String>, RampVehicleEvent, RampVehicleEvent> {

        // 存储车辆最近报告的匝道时间
        private transient ValueState<Long> lastRampTimeState;

        @Override
        public void open(Configuration parameters) {
            System.out.println("初始化匝道去重状态");
            // 状态TTL配置 - 保留2分钟
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(2))
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


            // 检查车辆是否在同一分钟内报告过
            Long lastTime = lastRampTimeState.value();
            long currentMinute = Instant.ofEpochMilli(event.timestamp)
                    .atZone(ZoneId.systemDefault())
                    .withSecond(0).withNano(0)
                    .toInstant().toEpochMilli();

            if (lastTime == null || lastTime < currentMinute) {
                // 首次在该分钟报告
                lastRampTimeState.update(currentMinute);
                out.collect(event);
            } else {
            }
        }
    }

    // 路段车辆去重处理函数 - 使用ValueState替代MapState解决OOM问题
    public static class SectionVehicleDeduplicationProcess
            extends KeyedProcessFunction<Long, SectionVehicleEvent, SectionVehicleEvent> {

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
            System.out.println("初始化路段去重状态");
            // 状态TTL配置 - 保留1小时（覆盖4个15分钟窗口）
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(2))
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
                SectionVehicleEvent event,
                Context ctx,
                Collector<SectionVehicleEvent> out) throws Exception {


            // 按分钟截断时间戳
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
                    } else {
                    }
                }
            }
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

    // 路段交通聚合函数
    public static class SectionTrafficAggregator implements org.apache.flink.api.common.functions.AggregateFunction<
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

            System.out.println("处理匝道窗口结果: rampId=" + key.f1);

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

    // 路段交通结果处理函数
    public static class SectionTrafficResultFunction extends ProcessWindowFunction<
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

            System.out.println("处理路段窗口结果: sectionId=" + key.f1);

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
            System.out.println("初始化HBase连接: table=" + tableName);

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
            System.out.println("写入HBase记录: rampId=" + value.f0);

            String timeStr;
            if ("minute".equals(timeGranularity)) {
                timeStr = Instant.ofEpochMilli(value.f1)
                        .atZone(ZoneId.systemDefault())
                        .format(MINUTE_ROWKEY_FORMATTER);
            } else {
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
                System.out.println("刷新HBase缓冲区: 已写入" + counter.get() + "条记录");
                mutator.flush();
            }
        }

        @Override
        public void close() throws Exception {
            System.out.println("关闭HBase连接");
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

    // 路段交通HBase Sink - 支持分钟时间粒度
    public static class SectionTrafficHBaseSink extends RichSinkFunction<Tuple8<Long, Integer, Long, Long, Long, Long, Double, Double>> {
        private final String tableName;
        private final String columnFamily;
        private final String timeGranularity;
        private transient Connection connection;
        private transient BufferedMutator mutator;
        private transient AtomicInteger counter;

        public SectionTrafficHBaseSink(String tableName, String columnFamily, String timeGranularity) {
            this.tableName = tableName;
            this.columnFamily = columnFamily;
            this.timeGranularity = timeGranularity;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("初始化HBase连接: table=" + tableName);

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
            System.out.println("写入HBase记录: sectionId=" + value.f1);

            String timeStr;
            if ("minute".equals(timeGranularity)) {
                timeStr = Instant.ofEpochMilli(value.f0)
                        .atZone(ZoneId.systemDefault())
                        .format(MINUTE_ROWKEY_FORMATTER);
            } else {
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
                System.out.println("刷新HBase缓冲区: 已写入" + counter.get() + "条记录");
                mutator.flush();
            }
        }

        @Override
        public void close() throws Exception {
            System.out.println("关闭HBase连接");
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