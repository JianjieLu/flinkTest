package whu.edu.ljj.flink.merge.Version1;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import whu.edu.ljj.flink.merge.Version1.Utils.PathPoint;
import lombok.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static whu.edu.ljj.flink.merge.Version1.Utils.convertToTimestampMillis;


public class SegCarIngestMoniOfiV5 {
    // 内存存储容器
    public static final ConcurrentHashMap<String, String> resultMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // Kafka配置保持不变
        String brokers = "100.65.38.40:9092";
        String groupId = "flink-group";
        List<String> topics = Arrays.asList("MergedPathData", "MergedPathData.sceneTest.1",
                "MergedPathData.sceneTest.2", "MergedPathData.sceneTest.3",
                "MergedPathData.sceneTest.4", "MergedPathData.sceneTest.5");

        // 初始化第一个KafkaSource
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics.get(0))
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.commit", "true")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> unionStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source 1");

        // 合并其他主题数据流
        for (int i = 1; i < topics.size(); i++) {
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics.get(i))
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setProperty("auto.offset.commit", "true")
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source " + (i + 1));
            unionStream = unionStream.union(stream);
        }

        // 数据解析处理
        DataStream<PathPoint> flatMapStream = unionStream
                .flatMap(new FlatMapFunction<String, PathPoint>() {
                    @Override
                    public void flatMap(String jsonString, Collector<PathPoint> out) {
                        try {
                            JSONObject jsonObject = JSON.parseObject(jsonString);
                            String timeStampStr = jsonObject.getString("timeStamp");
                            long timeObs = parseTimestamp(timeStampStr);

                            for (PathPoint ppoint : JSON.parseArray(jsonObject.getString("pathList"), PathPoint.class)) {
                                if (isValidStakeId(ppoint.getStakeId())) {
                                    out.collect(ppoint);
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("解析 JSON 时出错: " + e.getMessage());
                        }
                    }

                    private long parseTimestamp(String timeStampStr) throws Exception {
                        try {
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
                            LocalDateTime localDateTime = LocalDateTime.parse(timeStampStr, formatter);
                            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        } catch (Exception e) {
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
                            LocalDateTime localDateTime = LocalDateTime.parse(timeStampStr, formatter);
                            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                        }
                    }

                    private boolean isValidStakeId(String stakeId) {
                        return !stakeId.isEmpty() && !"ABCD".contains(Character.toString(stakeId.charAt(0)));
                    }
                });

        // 关键处理逻辑
        DataStream<Tuple2<String, String>> processedStream = flatMapStream
                .keyBy(ppoint -> generateRowKey(ppoint))
                .process(new VehicleAggregator())
                .returns(Types.TUPLE(Types.STRING, Types.STRING));

        // 添加内存存储Sink
        processedStream.addSink(new InMemoryMapSink());

        // 执行任务
        env.execute("Flink STCar to MemoryMap");
    }

    // 生成RowKey方法
    private static String generateRowKey(PathPoint ppoint) {
        long minuteTimestamp = convertToTimestampMillis(ppoint.getTimeStamp()) / 60000 * 60000;
        return minuteTimestamp + "_" + ppoint.getStakeId().split("\\+")[0];
    }

    // 内存存储Sink实现
    static class InMemoryMapSink extends RichSinkFunction<Tuple2<String, String>> {
        @Override
        public void invoke(Tuple2<String, String> value, Context context) {
            // 存储到内存Map
            resultMap.put(value.f0, value.f1);

            // 调试输出（可选）
            System.out.println("[Memory Storage] Key: " + value.f0);
            System.out.println("Value Length: " + value.f1.length() + " characters");
        }
    }

    // 车辆聚合处理器（保持不变）
    private static class VehicleAggregator extends KeyedProcessFunction<String, PathPoint, Tuple2<String, String>> {
        private transient MapState<Long, VehicleSeg> vehicleSegState;
        private transient ValueState<Boolean> timerState;
        private final long timerInterval = 60 * 1000;

        private final StateTtlConfig vehiclettlConfig = StateTtlConfig
                .newBuilder(Time.seconds(120))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<Long, VehicleSeg> vehicleSegDescriptor =
                    new MapStateDescriptor<>("vehicleState", Types.LONG, TypeInformation.of(VehicleSeg.class));
            vehicleSegDescriptor.enableTimeToLive(vehiclettlConfig);
            vehicleSegState = getRuntimeContext().getMapState(vehicleSegDescriptor);

            ValueStateDescriptor<Boolean> timerDesc =
                    new ValueStateDescriptor<>("timer-state", Boolean.class);
            timerState = getRuntimeContext().getState(timerDesc);
        }

        @Override
        public void processElement(PathPoint ppoint, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            if (!vehicleSegState.contains(ppoint.getId())) {
                VehicleSeg vehicleSeg = new VehicleSeg(
                        ppoint.getPlateNo(),
                        ppoint.getId(),
                        ppoint.getSpeed(),
                        ppoint.getDirection(),
                        1,
                        ppoint.getOriginalType(),
                        ppoint.getSpecialFlag()
                );
                vehicleSegState.put(ppoint.getId(), vehicleSeg);
            } else {
                VehicleSeg vehicleSeg = vehicleSegState.get(ppoint.getId());
                vehicleSeg.setSpeedSum(vehicleSeg.getSpeedSum() + ppoint.getSpeed());
                vehicleSeg.setPointSum(vehicleSeg.getPointSum() + 1);
            }

            if (timerState.value() == null || !timerState.value()) {
                ctx.timerService().registerProcessingTimeTimer(
                        ctx.timerService().currentProcessingTime() + timerInterval
                );
                timerState.update(true);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            if (!vehicleSegState.isEmpty()) {
                List<VehicleSeg> merged = new ArrayList<>();
                for (VehicleSeg seg : vehicleSegState.values()) {
                    merged.add(seg);
                }
                out.collect(new Tuple2<>(ctx.getCurrentKey(), JSON.toJSONString(merged)));
            }
            timerState.clear();
            vehicleSegState.clear();
        }
    }

    // 车辆分段数据实体类
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VehicleSeg {
        private String plateNo;
        private long carId;
        private float speedSum;
        private int direction;
        private int pointSum;
        private Integer originalType = null;
        private String specialFlag = null;
    }
}