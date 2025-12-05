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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
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
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.io.StringReader;

public class CombinedTrafficProcessingJob {

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

    // JSON解析器
    private static final JsonFactory jsonFactory = new JsonFactory();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置优化
        env.setParallelism(4);
        env.getConfig().enableObjectReuse();
        env.getConfig().setAutoWatermarkInterval(1000);

        // 设置检查点和状态后端
        env.enableCheckpointing(30000); // 30秒检查点间隔
        // env.setStateBackend(new RocksDBStateBackend("file:///tmp/checkpoints", true));

        String brokers = "10.48.53.82:9092";
        String rampGroupId = "ramp-traffic-group";
        String sectionGroupId = "fifteen-min-traffic-group";
        String rampTopic = "MergedRampPathData";
        String sectionTopics = "MergedPathData.sceneTest.1 MergedPathData.sceneTest.2 MergedPathData.sceneTest.3 MergedPathData.sceneTest.4 MergedPathData.sceneTest.5 MergedPathData.sceneTest.6 MergedPathData.sceneTest.7 MergedPathData.sceneTest.8 MergedPathData.sceneTest.9 MergedPathData.sceneTest.10 MergedPathData.sceneTest.11";

        // 创建匝道数据源
        KafkaSource<String> rampSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(rampTopic)
                .setGroupId(rampGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

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
                            try (JsonParser parser = jsonFactory.createParser(event)) {
                                while (parser.nextToken() != null) {
                                    if ("timeStamp".equals(parser.getCurrentName())) {
                                        parser.nextToken();
                                        String timeStampStr = parser.getText();
                                        LocalDateTime dateTime = LocalDateTime.parse(timeStampStr, JSON_TIME_FORMATTER);
                                        return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                                    }
                                }
                                return System.currentTimeMillis();
                            } catch (Exception e) {
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
                            try (JsonParser parser = jsonFactory.createParser(event)) {
                                while (parser.nextToken() != null) {
                                    if ("timeStamp".equals(parser.getCurrentName())) {
                                        parser.nextToken();
                                        String timeStampStr = parser.getText();
                                        LocalDateTime dateTime = LocalDateTime.parse(timeStampStr, JSON_TIME_FORMATTER);
                                        return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                                    }
                                }
                                return System.currentTimeMillis();
                            } catch (Exception e) {
                                return System.currentTimeMillis();
                            }
                        }),
                "Section Kafka Source"
        );

        // 处理匝道交通数据
        processRampTraffic(rampKafkaStream);

        // 处理路段交通数据
        processSectionTraffic(sectionKafkaStream);

        env.execute("Combined Traffic Processing Job");
    }

    // 处理匝道交通数据
    private static void processRampTraffic(DataStream<String> kafkaStream) {
        // 1. 解析JSON并提取匝道车辆数据，使用侧输出流处理错误数据
        SingleOutputStreamOperator<RampVehicleEvent> rampVehicleEvents = kafkaStream
                .process(new JsonToRampVehicleEventProcessFunction());

        // 获取解析错误的数据流
        DataStream<String> parseErrors = rampVehicleEvents.getSideOutput(PARSE_ERROR_TAG);
        // 可以在这里添加对错误数据的处理，例如写入日志或特定存储
        parseErrors.map(value -> {
            System.err.println("Ramp Parse Error: " + value);
            return value;
        }).setParallelism(1); // 限制并行度避免过多输出

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
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.minutes(5))
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
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.minutes(10))
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
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.minutes(15))
                .aggregate(new RampTrafficAggregator(), new RampTrafficResultFunction());

        // 8. 写入HBase - 小时数据
        hourlyRampTraffic.addSink(new RampTrafficHBaseSink("ramp_hour_traffic", "cf", "hourly")).setParallelism(2);

        // 9. 写入HBase - 天数据
        dailyRampTraffic.addSink(new RampTrafficHBaseSink("ramp_day_traffic", "cf", "daily")).setParallelism(2);

        // 10. 写入HBase - 月数据
        monthlyRampTraffic.addSink(new RampTrafficHBaseSink("ramp_month_traffic", "cf", "monthly")).setParallelism(2);
    }

    // 处理路段交通数据
    private static void processSectionTraffic(DataStream<String> kafkaStream) {
        // 1. 解析JSON并提取车辆轨迹数据，使用侧输出流处理错误数据
        SingleOutputStreamOperator<SectionVehicleEvent> vehicleEvents = kafkaStream
                .process(new JsonToSectionVehicleEventProcessFunction());

        // 获取解析错误的数据流
        DataStream<String> parseErrors = vehicleEvents.getSideOutput(PARSE_ERROR_TAG);
        // 可以在这里添加对错误数据的处理，例如写入日志或特定存储
        parseErrors.map(value -> {
            System.err.println("Section Parse Error: " + value);
            return value;
        }).setParallelism(1); // 限制并行度避免过多输出

        // 分配时间戳和水位线
        DataStream<SectionVehicleEvent> withTimestamps = vehicleEvents
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SectionVehicleEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner((event, ts) -> event.timestamp)
                );

        // 2. 按车辆ID分组，处理重复轨迹
        KeyedStream<SectionVehicleEvent, Long> keyedByVehicle = withTimestamps
                .keyBy(SectionVehicleEvent::getVehicleId);

        // 3. 处理车辆轨迹 - 确保每辆车在每个路段只计数一次
        DataStream<SectionVehicleEvent> deduplicatedEvents = keyedByVehicle
                .process(new SectionVehicleDeduplicationProcess());

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
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.minutes(5))
                .aggregate(new SectionTrafficAggregator(), new SectionTrafficResultFunction());

        // 6. 写入HBase - 15分钟数据
        trafficFlow.addSink(new SectionTrafficHBaseSink("FifteenMinuteTrafficFlow", "cf", "minute")).setParallelism(2);

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
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.minutes(5))
                .aggregate(new SectionTrafficAggregator(), new SectionTrafficResultFunction());

        // 8. 写入HBase - 小时数据
        hourlyTraffic.addSink(new SectionTrafficHBaseSink("AnaHourlyTrafficFlow", "cf", "hour")).setParallelism(2);

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
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.minutes(10))
                .aggregate(new SectionTrafficAggregator(), new SectionTrafficResultFunction());

        // 10. 写入HBase - 天数据
        dailyTraffic.addSink(new SectionTrafficHBaseSink("AnaDailyTrafficFlow", "cf", "day")).setParallelism(2);

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
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.minutes(15))
                .aggregate(new SectionTrafficAggregator(), new SectionTrafficResultFunction());

        // 12. 写入HBase - 月数据
        monthlyTraffic.addSink(new SectionTrafficHBaseSink("AnaMonthlyTrafficFlow", "cf", "month")).setParallelism(2);
    }

    // JSON解析处理函数，使用侧输出流处理错误 - 匝道数据
    public static class JsonToRampVehicleEventProcessFunction
            extends org.apache.flink.streaming.api.functions.ProcessFunction<String, RampVehicleEvent> {

        @Override
        public void processElement(
                String jsonString,
                org.apache.flink.streaming.api.functions.ProcessFunction<String, RampVehicleEvent>.Context ctx,
                Collector<RampVehicleEvent> out) {
            try (JsonParser parser = jsonFactory.createParser(jsonString)) {
                long eventTimestamp = 0;
                boolean inPathList = false;
                Long vehicleId = null;
                String stakeId = null;
                Double speed = null;
                Integer originalType = null;

                while (parser.nextToken() != null) {
                    JsonToken token = parser.getCurrentToken();
                    String fieldName = parser.getCurrentName();

                    if (token == JsonToken.FIELD_NAME) {
                        if ("timeStamp".equals(fieldName)) {
                            parser.nextToken();
                            String timeStampStr = parser.getText();
                            eventTimestamp = LocalDateTime.parse(timeStampStr, JSON_TIME_FORMATTER)
                                    .atZone(ZoneId.systemDefault())
                                    .toInstant()
                                    .toEpochMilli();
                        } else if ("pathList".equals(fieldName)) {
                            parser.nextToken(); // 移动到START_ARRAY
                            inPathList = true;
                        } else if (inPathList && token == JsonToken.START_OBJECT) {
                            // 重置车辆信息
                            vehicleId = null;
                            stakeId = null;
                            speed = null;
                            originalType = null;
                        } else if (inPathList && fieldName != null) {
                            parser.nextToken();
                            if ("id".equals(fieldName)) {
                                vehicleId = parser.getLongValue();
                            } else if ("stakeId".equals(fieldName)) {
                                stakeId = parser.getText();
                            } else if ("speed".equals(fieldName)) {
                                speed = parser.getDoubleValue();
                            } else if ("originalType".equals(fieldName)) {
                                originalType = parser.getIntValue();
                            }
                        }
                    } else if (inPathList && token == JsonToken.END_OBJECT) {
                        // 处理完一个车辆对象
                        if (vehicleId != null && stakeId != null && speed != null && originalType != null) {
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
                    } else if (token == JsonToken.END_ARRAY && inPathList) {
                        inPathList = false;
                    }
                }
            } catch (Exception e) {
                // 将解析错误的数据输出到侧输出流
                ctx.output(PARSE_ERROR_TAG, "JSON解析错误: " + e.getMessage() + "\n原始数据: " +
                        (jsonString.length() > 200 ? jsonString.substring(0, 200) + "..." : jsonString));
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

    // JSON解析处理函数，使用侧输出流处理错误 - 路段数据
    public static class JsonToSectionVehicleEventProcessFunction
            extends org.apache.flink.streaming.api.functions.ProcessFunction<String, SectionVehicleEvent> {

        @Override
        public void processElement(
                String jsonString,
                org.apache.flink.streaming.api.functions.ProcessFunction<String, SectionVehicleEvent>.Context ctx,
                Collector<SectionVehicleEvent> out) {
            try (JsonParser parser = jsonFactory.createParser(jsonString)) {
                long eventTimestamp = 0;
                boolean inPathList = false;
                Long vehicleId = null;
                String stakeId = null;
                Integer direction = null;
                Double speed = null;
                Integer vehicleType = null;

                while (parser.nextToken() != null) {
                    JsonToken token = parser.getCurrentToken();
                    String fieldName = parser.getCurrentName();

                    if (token == JsonToken.FIELD_NAME) {
                        if ("timeStamp".equals(fieldName)) {
                            parser.nextToken();
                            String timeStampStr = parser.getText();
                            eventTimestamp = LocalDateTime.parse(timeStampStr, JSON_TIME_FORMATTER)
                                    .atZone(ZoneId.systemDefault())
                                    .toInstant()
                                    .toEpochMilli();
                        } else if ("pathList".equals(fieldName)) {
                            parser.nextToken(); // 移动到START_ARRAY
                            inPathList = true;
                        } else if (inPathList && token == JsonToken.START_OBJECT) {
                            // 重置车辆信息
                            vehicleId = null;
                            stakeId = null;
                            direction = null;
                            speed = null;
                            vehicleType = null;
                        } else if (inPathList && fieldName != null) {
                            parser.nextToken();
                            if ("id".equals(fieldName)) {
                                vehicleId = parser.getLongValue();
                            } else if ("stakeId".equals(fieldName)) {
                                stakeId = parser.getText();
                            } else if ("direction".equals(fieldName)) {
                                direction = parser.getIntValue();
                            } else if ("speed".equals(fieldName)) {
                                speed = parser.getDoubleValue();
                            } else if ("vehicleType".equals(fieldName)) {
                                vehicleType = parser.getIntValue();
                            }
                        }
                    } else if (inPathList && token == JsonToken.END_OBJECT) {
                        // 处理完一个车辆对象
                        if (vehicleId != null && stakeId != null && direction != null &&
                                speed != null && vehicleType != null) {
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
                            }
                        }
                    } else if (token == JsonToken.END_ARRAY && inPathList) {
                        inPathList = false;
                    }
                }
            } catch (Exception e) {
                // 将解析错误的数据输出到侧输出流
                ctx.output(PARSE_ERROR_TAG, "JSON解析错误: " + e.getMessage() + "\n原始数据: " +
                        (jsonString.length() > 200 ? jsonString.substring(0, 200) + "..." : jsonString));
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
            // 状态TTL配置 - 保留12小时（比之前缩短）
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(12))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupFullSnapshot()
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
            // 状态TTL配置 - 保留2小时（比之前缩短）
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupFullSnapshot()
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
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.client.write.buffer", "1048576"); // 减少到1MB
            conf.set("hbase.rpc.timeout", "30000");
            conf.set("hbase.client.operation.timeout", "30000");

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
                }
            } catch (Exception e) {
                System.err.println("HBase表检查失败: " + e.getMessage());
            }

            BufferedMutatorParams params = new BufferedMutatorParams(hbaseTable)
                    .writeBufferSize(1 * 1024 * 1024); // 减少到1MB
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
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("track_count"), Bytes.toBytes(String.valueOf(value.f4)));

            String avgSpeed = String.format("%.1f", value.f5);
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("avg_speed"), Bytes.toBytes(avgSpeed));

            mutator.mutate(put);

            // 每50条记录刷新一次（更频繁）
            if (counter.incrementAndGet() % 50 == 0) {
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

    // 路段交通HBase Sink - 支持不同时间粒度
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
            org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "100.65.38.139,100.65.38.140,100.65.38.141,100.65.38.142,10.48.53.80");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.client.write.buffer", "1048576"); // 减少到1MB
            conf.set("hbase.rpc.timeout", "30000");
            conf.set("hbase.client.operation.timeout", "30000");

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
                }
            } catch (Exception e) {
                System.err.println("HBase表检查失败: " + e.getMessage());
            }

            BufferedMutatorParams params = new BufferedMutatorParams(hbaseTable)
                    .writeBufferSize(1 * 1024 * 1024); // 减少到1MB
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
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("up_track"), Bytes.toBytes(String.valueOf(value.f3)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("down_bus"), Bytes.toBytes(String.valueOf(value.f4)));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("down_track"), Bytes.toBytes(String.valueOf(value.f5)));

            String upAvgSpeed = String.format("%.1f", value.f6);
            String downAvgSpeed = String.format("%.1f", value.f7);
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("up_avg_speed"), Bytes.toBytes(upAvgSpeed));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("down_avg_speed"), Bytes.toBytes(downAvgSpeed));

            mutator.mutate(put);

            // 每50条记录刷新一次（更频繁）
            if (counter.incrementAndGet() % 50 == 0) {
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