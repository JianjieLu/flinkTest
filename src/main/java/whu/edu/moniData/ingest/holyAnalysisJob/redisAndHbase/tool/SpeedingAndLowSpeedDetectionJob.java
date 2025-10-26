package whu.edu.moniData.ingest.holyAnalysisJob.redisAndHbase.tool;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 完整的超速和低速检测Flink作业
 * 功能：实时检测车辆超速和低速行驶事件
 */
public class SpeedingAndLowSpeedDetectionJob {

    // 超速阈值 100km/h
    private static final double SPEED_THRESHOLD = 100.0;
    // 低速阈值 85km/h
    private static final double LOW_SPEED_THRESHOLD = 85.0;

    // 事件最小持续时间（毫秒）
    private static final long MIN_EVENT_DURATION = 3000;

    /**
     * 基础事件信息类
     */
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class BaseEvent {
        // 主要信息
        private String vehicleId;
        private String plateNo;
        private String eventType; // 事件类型: speeding 或 low_speed

        // 时间信息
        private long startTime;
        private long endTime;
        private long duration;

        // 位置信息
        private double startMileage;  // 起始桩号
        private double endMileage;    // 结束桩号
        private int direction;
        private int laneNo;           // 车道号

        // 速度信息
        private double minSpeed;      // 最小速度（km/h）
        private double maxSpeed;      // 最大速度（km/h）
        private double avgSpeed;      // 平均速度（km/h）
        private double preSpeed;      // 事件前一帧的速度
        private double postSpeed;     // 事件后一帧的速度

        @Override
        public String toString() {
            return String.format("%s事件: 车辆ID=%s, 持续时间=%dms, 起始桩号=%.3f, 结束桩号=%.3f, " +
                            "最小速度=%.2fkm/h, 最大速度=%.2fkm/h, 前速=%.2fkm/h, 后速=%.2fkm/h, " +
                            "方向=%d, 车道=%d, 开始时间=%d, 结束时间=%d",
                    eventType, vehicleId, duration, startMileage, endMileage,
                    minSpeed, maxSpeed, preSpeed, postSpeed,
                    direction, laneNo, startTime, endTime);
        }
    }

    /**
     * 轨迹点数据类
     */
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class TrajectoryPoint {
        private double speed;        // 速度 km/h
        private double mileage;      // 里程
        private int laneNo;          // 车道号
        private int direction;       // 方向
        private long timestamp;      // 时间戳
    }

    /**
     * 事件状态基类
     */
    public static abstract class BaseEventState {
        boolean isInEvent = false;
        String vehicleId;
        String plateNo;

        // 时间信息
        long startTime;
        long endTime;
        long lastSeenTime;

        // 位置信息
        double startMileage;
        double endMileage;
        int direction;
        int laneNo;

        // 速度信息
        double minSpeed;
        double maxSpeed;
        double speedSum;
        double preSpeed;     // 事件前一帧速度
        double postSpeed;    // 事件后一帧速度
        int pointCount;

        // 轨迹点列表
        List<TrajectoryPoint> eventPoints;

        abstract void reset();
    }

    /**
     * 超速事件状态类
     */
    public static class SpeedingEventState extends BaseEventState {
        @Override
        void reset() {
            isInEvent = false;
            vehicleId = null;
            plateNo = null;
            startTime = 0;
            endTime = 0;
            lastSeenTime = 0;
            startMileage = 0;
            endMileage = 0;
            direction = -1;
            laneNo = -1;
            minSpeed = 0;
            maxSpeed = 0;
            speedSum = 0;
            preSpeed = 0;
            postSpeed = 0;
            pointCount = 0;
            eventPoints = null;
        }

        @Override
        public String toString() {
            return String.format("SpeedingEventState{vehicleId=%s, isInEvent=%s, points=%d, duration=%dms}",
                    vehicleId, isInEvent, pointCount, (endTime - startTime));
        }
    }

    /**
     * 低速事件状态类
     */
    public static class LowSpeedEventState extends BaseEventState {
        @Override
        void reset() {
            isInEvent = false;
            vehicleId = null;
            plateNo = null;
            startTime = 0;
            endTime = 0;
            lastSeenTime = 0;
            startMileage = 0;
            endMileage = 0;
            direction = -1;
            laneNo = -1;
            minSpeed = 0;
            maxSpeed = 0;
            speedSum = 0;
            preSpeed = 0;
            postSpeed = 0;
            pointCount = 0;
            eventPoints = null;
        }

        @Override
        public String toString() {
            return String.format("LowSpeedEventState{vehicleId=%s, isInEvent=%s, points=%d, duration=%dms}",
                    vehicleId, isInEvent, pointCount, (endTime - startTime));
        }
    }

    /**
     * 超速和低速检测处理器
     */
    public static class SpeedEventDetector extends RichFlatMapFunction<String, BaseEvent> {

        private transient ValueState<SpeedingEventState> speedingState;
        private transient ValueState<LowSpeedEventState> lowSpeedState;
        private transient ValueState<TrajectoryPoint> lastNormalPointState; // 存储上一个正常速度点

        @Override
        public void open(Configuration parameters) throws Exception {
            // 超速状态
            ValueStateDescriptor<SpeedingEventState> speedingDescriptor =
                    new ValueStateDescriptor<>("speeding-state", SpeedingEventState.class);
            speedingState = getRuntimeContext().getState(speedingDescriptor);

            // 低速状态
            ValueStateDescriptor<LowSpeedEventState> lowSpeedDescriptor =
                    new ValueStateDescriptor<>("low-speed-state", LowSpeedEventState.class);
            lowSpeedState = getRuntimeContext().getState(lowSpeedDescriptor);

            // 上一个正常点状态
            ValueStateDescriptor<TrajectoryPoint> lastPointDescriptor =
                    new ValueStateDescriptor<>("last-point-state", TrajectoryPoint.class);
            lastNormalPointState = getRuntimeContext().getState(lastPointDescriptor);
        }

        @Override
        public void flatMap(String jsonString, Collector<BaseEvent> out) throws Exception {
            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                String timeStamp = jsonObject.optString("timeStamp", "");
                JSONArray pathList = jsonObject.optJSONArray("pathList");

                if (pathList == null || timeStamp.isEmpty()) {
                    return;
                }

                long batchTimestamp = parseTimestamp(timeStamp);
                if (batchTimestamp == 0) {
                    System.err.println("无法解析批次时间戳: " + timeStamp);
                    return;
                }

                for (int i = 0; i < pathList.length(); i++) {
                    JSONObject vehicleData = pathList.getJSONObject(i);
                    if (vehicleData != null) {
                        processVehicleData(vehicleData, batchTimestamp, out);
                    }
                }

                // 检查超时车辆
                checkTimeoutVehicles(batchTimestamp, out);

            } catch (Exception e) {
                System.err.println("速度事件检测处理异常: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private void processVehicleData(JSONObject vehicleData, long batchTimestamp,
                                        Collector<BaseEvent> out) throws Exception {
            String vehicleId = String.valueOf(vehicleData.optLong("id", -1));
            String plateNo = vehicleData.optString("plateNo", "");
            double speed = vehicleData.optDouble("speed", 0.0); // km/h单位
            int direction = vehicleData.optInt("direction", -1);
            int laneNo = vehicleData.optInt("laneNo", -1);
            double mileage = vehicleData.optDouble("mileage", 0.0) / 1000.0; // 转换为桩号

            if (vehicleId.equals("-1")) {
                return;
            }

            // 创建当前轨迹点
            TrajectoryPoint currentPoint = new TrajectoryPoint(speed, mileage, laneNo, direction, batchTimestamp);

            // 获取状态
            SpeedingEventState speedingStateValue = speedingState.value();
            LowSpeedEventState lowSpeedStateValue = lowSpeedState.value();
            if (speedingStateValue == null) {
                speedingStateValue = new SpeedingEventState();
            }
            if (lowSpeedStateValue == null) {
                lowSpeedStateValue = new LowSpeedEventState();
            }

            // 获取上一个正常速度点
            TrajectoryPoint lastNormalPoint = lastNormalPointState.value();

            // 更新最后可见时间
            speedingStateValue.lastSeenTime = batchTimestamp;
            lowSpeedStateValue.lastSeenTime = batchTimestamp;

            // 检测超速和低速
            if (speed > SPEED_THRESHOLD) {
                handleSpeeding(vehicleId, plateNo, currentPoint, lastNormalPoint, speedingStateValue);
                // 如果同时存在低速事件，结束低速事件
                handleNormalSpeedForLowSpeed(vehicleId, currentPoint, out, lowSpeedStateValue);
            } else if (speed < LOW_SPEED_THRESHOLD) {
                handleLowSpeed(vehicleId, plateNo, currentPoint, lastNormalPoint, lowSpeedStateValue);
                // 如果同时存在超速事件，结束超速事件
                handleNormalSpeedForSpeeding(vehicleId, currentPoint, out, speedingStateValue);
            } else {
                // 正常速度范围，结束可能存在的超速或低速事件
                handleNormalSpeedForSpeeding(vehicleId, currentPoint, out, speedingStateValue);
                handleNormalSpeedForLowSpeed(vehicleId, currentPoint, out, lowSpeedStateValue);
            }

            // 更新状态
            speedingState.update(speedingStateValue);
            lowSpeedState.update(lowSpeedStateValue);
            lastNormalPointState.update(currentPoint);
        }

        private void handleSpeeding(String vehicleId, String plateNo, TrajectoryPoint currentPoint,
                                    TrajectoryPoint lastNormalPoint, SpeedingEventState state) {
            if (!state.isInEvent) {
                // 开始新的超速事件
                System.out.println("检测到车辆超速开始: " + vehicleId + ", 速度: " + currentPoint.getSpeed() + "km/h");

                state.isInEvent = true;
                state.vehicleId = vehicleId;
                state.plateNo = plateNo;
                state.startTime = currentPoint.getTimestamp();
                state.endTime = currentPoint.getTimestamp();
                state.startMileage = currentPoint.getMileage();
                state.endMileage = currentPoint.getMileage();
                state.direction = currentPoint.getDirection();
                state.laneNo = currentPoint.getLaneNo();

                // 速度相关初始化
                state.minSpeed = currentPoint.getSpeed();
                state.maxSpeed = currentPoint.getSpeed();
                state.speedSum = currentPoint.getSpeed();
                state.pointCount = 1;

                // 记录超速前一帧的速度
                if (lastNormalPoint != null) {
                    state.preSpeed = lastNormalPoint.getSpeed();
                } else {
                    state.preSpeed = 0.0;
                }

                // 初始化轨迹点列表
                state.eventPoints = new ArrayList<>();
                state.eventPoints.add(currentPoint);

            } else {
                // 继续当前超速事件
                state.endTime = currentPoint.getTimestamp();
                state.endMileage = currentPoint.getMileage();

                // 更新速度统计
                state.minSpeed = Math.min(state.minSpeed, currentPoint.getSpeed());
                state.maxSpeed = Math.max(state.maxSpeed, currentPoint.getSpeed());
                state.speedSum += currentPoint.getSpeed();
                state.pointCount++;

                // 更新车道号
                state.laneNo = currentPoint.getLaneNo();

                // 添加轨迹点
                state.eventPoints.add(currentPoint);
            }
        }

        private void handleLowSpeed(String vehicleId, String plateNo, TrajectoryPoint currentPoint,
                                    TrajectoryPoint lastNormalPoint, LowSpeedEventState state) {
            if (!state.isInEvent) {
                // 开始新的低速事件
                System.out.println("检测到车辆低速开始: " + vehicleId + ", 速度: " + currentPoint.getSpeed() + "km/h");

                state.isInEvent = true;
                state.vehicleId = vehicleId;
                state.plateNo = plateNo;
                state.startTime = currentPoint.getTimestamp();
                state.endTime = currentPoint.getTimestamp();
                state.startMileage = currentPoint.getMileage();
                state.endMileage = currentPoint.getMileage();
                state.direction = currentPoint.getDirection();
                state.laneNo = currentPoint.getLaneNo();

                // 速度相关初始化
                state.minSpeed = currentPoint.getSpeed();
                state.maxSpeed = currentPoint.getSpeed();
                state.speedSum = currentPoint.getSpeed();
                state.pointCount = 1;

                // 记录低速前一帧的速度
                if (lastNormalPoint != null) {
                    state.preSpeed = lastNormalPoint.getSpeed();
                } else {
                    state.preSpeed = 0.0;
                }

                // 初始化轨迹点列表
                state.eventPoints = new ArrayList<>();
                state.eventPoints.add(currentPoint);

            } else {
                // 继续当前低速事件
                state.endTime = currentPoint.getTimestamp();
                state.endMileage = currentPoint.getMileage();

                // 更新速度统计
                state.minSpeed = Math.min(state.minSpeed, currentPoint.getSpeed());
                state.maxSpeed = Math.max(state.maxSpeed, currentPoint.getSpeed());
                state.speedSum += currentPoint.getSpeed();
                state.pointCount++;

                // 更新车道号
                state.laneNo = currentPoint.getLaneNo();

                // 添加轨迹点
                state.eventPoints.add(currentPoint);
            }
        }

        private void handleNormalSpeedForSpeeding(String vehicleId, TrajectoryPoint currentPoint,
                                                  Collector<BaseEvent> out, SpeedingEventState state) throws Exception {
            if (state.isInEvent && state.vehicleId.equals(vehicleId)) {
                // 超速结束，记录超速后一帧的速度
                state.postSpeed = currentPoint.getSpeed();

                // 检查是否满足最小持续时间
                long duration = state.endTime - state.startTime;
                if (duration >= MIN_EVENT_DURATION) {
                    // 计算平均速度
                    double avgSpeed = state.speedSum / state.pointCount;

                    // 输出超速事件
                    BaseEvent event = new BaseEvent();
                    event.setVehicleId(state.vehicleId);
                    event.setPlateNo(state.plateNo);
                    event.setEventType("speeding");

                    event.setDuration(duration);
                    event.setStartMileage(state.startMileage);
                    event.setEndMileage(state.endMileage);
                    event.setMinSpeed(state.minSpeed);
                    event.setMaxSpeed(state.maxSpeed);
                    event.setPreSpeed(state.preSpeed);
                    event.setPostSpeed(state.postSpeed);
                    event.setDirection(state.direction);
                    event.setLaneNo(state.laneNo);
                    event.setStartTime(state.startTime);
                    event.setEndTime(state.endTime);
                    event.setAvgSpeed(avgSpeed);

                    out.collect(event);
                    System.out.println("输出超速事件: " + event);
                } else {
                    System.out.println("超速持续时间不足，忽略: " + vehicleId + ", 持续时间: " + duration + "ms");
                }

                // 重置状态
                state.reset();
                speedingState.update(state);
            }
        }

        private void handleNormalSpeedForLowSpeed(String vehicleId, TrajectoryPoint currentPoint,
                                                  Collector<BaseEvent> out, LowSpeedEventState state) throws Exception {
            if (state.isInEvent && state.vehicleId.equals(vehicleId)) {
                // 低速结束，记录低速后一帧的速度
                state.postSpeed = currentPoint.getSpeed();

                // 检查是否满足最小持续时间
                long duration = state.endTime - state.startTime;
                if (duration >= MIN_EVENT_DURATION) {
                    // 计算平均速度
                    double avgSpeed = state.speedSum / state.pointCount;

                    // 输出低速事件
                    BaseEvent event = new BaseEvent();
                    event.setVehicleId(state.vehicleId);
                    event.setPlateNo(state.plateNo);
                    event.setEventType("low_speed");

                    event.setDuration(duration);
                    event.setStartMileage(state.startMileage);
                    event.setEndMileage(state.endMileage);
                    event.setMinSpeed(state.minSpeed);
                    event.setMaxSpeed(state.maxSpeed);
                    event.setPreSpeed(state.preSpeed);
                    event.setPostSpeed(state.postSpeed);
                    event.setDirection(state.direction);
                    event.setLaneNo(state.laneNo);
                    event.setStartTime(state.startTime);
                    event.setEndTime(state.endTime);
                    event.setAvgSpeed(avgSpeed);

                    out.collect(event);
                    System.out.println("输出低速事件: " + event);
                } else {
                    System.out.println("低速持续时间不足，忽略: " + vehicleId + ", 持续时间: " + duration + "ms");
                }

                // 重置状态
                state.reset();
                lowSpeedState.update(state);
            }
        }

        private void checkTimeoutVehicles(long currentTime, Collector<BaseEvent> out) throws Exception {
            // 检查超速事件超时
            SpeedingEventState speedingStateValue = speedingState.value();
            if (speedingStateValue != null && speedingStateValue.isInEvent) {
                long timeSinceLastUpdate = currentTime - speedingStateValue.lastSeenTime;
                if (timeSinceLastUpdate > 60000) { // 60秒超时
                    System.out.println("清理超时超速事件: " + speedingStateValue.vehicleId);
                    handleTimeoutEvent(speedingStateValue, out, "speeding_timeout");
                    speedingStateValue.reset();
                    speedingState.update(speedingStateValue);
                }
            }

            // 检查低速事件超时
            LowSpeedEventState lowSpeedStateValue = lowSpeedState.value();
            if (lowSpeedStateValue != null && lowSpeedStateValue.isInEvent) {
                long timeSinceLastUpdate = currentTime - lowSpeedStateValue.lastSeenTime;
                if (timeSinceLastUpdate > 60000) { // 60秒超时
                    System.out.println("清理超时低速事件: " + lowSpeedStateValue.vehicleId);
                    handleTimeoutEvent(lowSpeedStateValue, out, "low_speed_timeout");
                    lowSpeedStateValue.reset();
                    lowSpeedState.update(lowSpeedStateValue);
                }
            }
        }

        private void handleTimeoutEvent(BaseEventState state, Collector<BaseEvent> out, String eventType) throws Exception {
            if (state.pointCount > 0) {
                state.postSpeed = 0.0; // 超时情况下后一帧速度设为0
                long duration = state.endTime - state.startTime;

                if (duration >= MIN_EVENT_DURATION) {
                    double avgSpeed = state.speedSum / state.pointCount;

                    BaseEvent event = new BaseEvent();
                    event.setVehicleId(state.vehicleId);
                    event.setPlateNo(state.plateNo);
                    event.setEventType(eventType);

                    event.setDuration(duration);
                    event.setStartMileage(state.startMileage);
                    event.setEndMileage(state.endMileage);
                    event.setMinSpeed(state.minSpeed);
                    event.setMaxSpeed(state.maxSpeed);
                    event.setPreSpeed(state.preSpeed);
                    event.setPostSpeed(state.postSpeed);
                    event.setDirection(state.direction);
                    event.setLaneNo(state.laneNo);
                    event.setStartTime(state.startTime);
                    event.setEndTime(state.endTime);
                    event.setAvgSpeed(avgSpeed);

                    out.collect(event);
                    System.out.println("输出超时事件: " + event);
                }
            }
        }

        private long parseTimestamp(String timestampStr) {
            try {
                DateTimeFormatter[] formatters = {
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS"),
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS"),
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                };

                for (DateTimeFormatter formatter : formatters) {
                    try {
                        LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
                        return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    } catch (Exception e) {
                        // 继续尝试下一种格式
                    }
                }

                if (timestampStr.contains("T")) {
                    return LocalDateTime.parse(timestampStr.replace(" ", "T"))
                            .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                }

                System.err.println("无法解析时间戳: " + timestampStr);
                return 0;
            } catch (Exception e) {
                System.err.println("时间戳解析异常: " + e.getMessage());
                return 0;
            }
        }
    }

    /**
     * 事件Kafka序列化器 - 支持超速和低速事件
     */
    public static class EventKafkaSerializer implements KafkaRecordSerializationSchema<BaseEvent> {

        @Override
        public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) {
            // 初始化逻辑
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(BaseEvent event, KafkaSinkContext context, Long timestamp) {
            try {
                JSONObject json = new JSONObject();

                // 严格按照要求的顺序添加字段
                json.put("duration", event.getDuration());
                json.put("startMileage", event.getStartMileage());
                json.put("endMileage", event.getEndMileage());
                json.put("minSpeed", event.getMinSpeed());
                json.put("maxSpeed", event.getMaxSpeed());
                json.put("preSpeed", event.getPreSpeed());
                json.put("postSpeed", event.getPostSpeed());
                json.put("direction", event.getDirection());
                json.put("laneNo", event.getLaneNo());
                json.put("startTime", event.getStartTime());
                json.put("endTime", event.getEndTime());
                json.put("eventType", event.getEventType());

                // 附加信息
                json.put("vehicleId", event.getVehicleId());
                json.put("plateNo", event.getPlateNo());
                json.put("avgSpeed", event.getAvgSpeed());
                json.put("processTime", System.currentTimeMillis());

                String topic = "speeding_events";
                if ("low_speed".equals(event.getEventType()) || "low_speed_timeout".equals(event.getEventType())) {
                    topic = "low_speed_events";
                }

                String jsonString = json.toString();
                return new ProducerRecord<>(
                        topic,
                        null,
                        event.getStartTime(),
                        null,
                        jsonString.getBytes(StandardCharsets.UTF_8)
                );
            } catch (Exception e) {
                System.err.println("序列化事件失败: " + e.getMessage());
                return null;
            }
        }
    }

    /**
     * 创建Kafka数据源
     */
    private static DataStream<String> createKafkaSource(StreamExecutionEnvironment env) {
        String brokers = "10.48.53.82:9092";
        String groupId = "speed-detection-group";

        List<String> topics = Arrays.asList(
                "jtkj.jga.path"
        );

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty("auto.offset.reset", "latest")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }

    /**
     * 创建Kafka Sink用于输出事件
     */
    private static KafkaSink<BaseEvent> createKafkaSink() {
        String brokers = "10.48.53.82:9092";

        return KafkaSink.<BaseEvent>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(new EventKafkaSerializer())
                .build();
    }

    /**
     * 主方法 - 完整的Flink作业入口
     */
    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        System.out.println("启动超速和低速检测Flink作业...");

        // 1. 创建数据源
        DataStream<String> kafkaStream = createKafkaSource(env);

        // 2. 处理数据流 - 按车辆ID分组并进行速度事件检测
        SingleOutputStreamOperator<BaseEvent> speedEvents = kafkaStream
                .keyBy(data -> {
                    // 提取车辆ID作为key
                    try {
                        JSONObject json = new JSONObject(data);
                        JSONArray pathList = json.optJSONArray("pathList");
                        if (pathList != null && pathList.length() > 0) {
                            JSONObject firstVehicle = pathList.getJSONObject(0);
                            return String.valueOf(firstVehicle.optLong("id", -1));
                        }
                    } catch (Exception e) {
                        // 忽略解析错误
                    }
                    return "unknown";
                })
                .flatMap(new SpeedEventDetector())
                .name("速度事件检测处理器")
                .setParallelism(5);

        // 3. 输出结果
        // 3.1 输出到Kafka（超速和低速事件会发送到不同的topic）
        KafkaSink<BaseEvent> kafkaSink = createKafkaSink();
        speedEvents.sinkTo(kafkaSink)
                .name("速度事件Kafka输出")
                .setParallelism(2);

        // 3.2 输出到控制台（用于调试）
        speedEvents.print()
                .name("速度事件控制台输出")
                .setParallelism(1);

        // 4. 执行作业
        System.out.println("开始执行Flink速度事件检测作业...");
        env.execute("实时车辆超速和低速检测作业");

        System.out.println("Flink作业执行完成");
    }
}