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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

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

    // 事件类型编码（根据《事件类型列表编码.doc》）
    private static final int EVENT_TYPE_SPEEDING = 1001; // 超速事件编码，需根据实际文档调整
    private static final int EVENT_TYPE_LOW_SPEED = 1002; // 低速事件编码，需根据实际文档调整

    // 事件等级编码
    private static final String EVENT_LEVEL_GENERAL = "10010001";    // 一般
    private static final String EVENT_LEVEL_LARGER = "10010002";     // 较大
    private static final String EVENT_LEVEL_MAJOR = "10010003";      // 重大
    private static final String EVENT_LEVEL_EXTREME = "10010004";    // 特别重大

    // 事件源编码
    private static final String EVENT_SOURCE_VIDEO = "2"; // 视频分析

    private static final AtomicLong EVENT_ID_GENERATOR = new AtomicLong(1000000000L);

    /**
     * 车辆信息类（carList中的车辆信息）
     */
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class EventVehicle {
        private Integer carId;           // 车辆id
        private String plateNo;          // 车牌号
        private Integer plateColor;      // 车牌颜色
        private Integer vehicleType;     // 车型
        private String specialFlag;      // 特殊车辆类型

        public JSONObject toJSONObject() {
            JSONObject json = new JSONObject();
            json.put("carId", carId != null ? carId : JSONObject.NULL);
            json.put("plateNo", plateNo != null ? plateNo : JSONObject.NULL);
            json.put("plateColor", plateColor != null ? plateColor : JSONObject.NULL);
            json.put("vehicleType", vehicleType != null ? vehicleType : JSONObject.NULL);
            json.put("specialFlag", specialFlag != null ? specialFlag : JSONObject.NULL);
            return json;
        }
    }

    /**
     * 标准事件信息类（符合文档格式）
     */
    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    public static class StandardEvent {
        // 主要信息
        private Integer eventId;             // 事件id（唯一性）
        private String timeStamp;           // 事件上报时间戳
        private Integer eventType;          // 事件类型

        // 位置信息
        private String startStake;          // 事件起始桩号
        private String endStake;            // 事件结束桩号
        private Integer startMileage;       // 事件范围起始里程
        private Integer endMilage;          // 事件范围截止里程
        private Float startLongitude;       // 事件范围起始经度
        private Float startLatitude;        // 事件范围起始纬度
        private Float endLongitude;         // 事件范围截止经度
        private Float endLatitude;          // 事件范围截止纬度

        // 事件详情
        private String laneNo;              // 事件所在车道号
        private Integer direction;          // 事件发生区域的行车方向
        private JSONArray carList;          // 事件发生车辆集合

        // 附加信息
        private String eventLevel;          // 事件等级
        private String eventDes;            // 事件概要描述
        private String eventReason;         // 事件原因
        private String eventPicPath;        // 事件现场图片路径
        private String eventVideoPath;      // 事件现场视频路径
        private String eventSource;         // 事件源
        private String sourceRemark;        // 来源备注
        private String waySectionId;        // 路段id
        private String waySectionName;      // 路段名称
        private Boolean manualAudit;        // 事件是否需要人工审核校验
        private Integer constructionVehicles; // 施工类事件，施工车数量
        private Integer constructionPerson;   // 施工类事件，施工人员数量

        @Override
        public String toString() {
            return String.format("标准事件: ID=%d, 类型=%d, 时间=%s, 路段=%s, 车道=%s, 方向=%d, 车辆数=%d",
                    eventId, eventType, timeStamp, waySectionName, laneNo, direction,
                    carList != null ? carList.length() : 0);
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
        private double speed;           // 速度 km/h
        private double mileage;         // 里程
        private int laneNo;             // 车道号
        private int direction;          // 方向
        private long timestamp;         // 时间戳
        private float longitude;        // 经度
        private float latitude;         // 纬度
        private String stakeId;         // 桩号
        private Integer vehicleType;    // 车型
        private Integer plateColor;     // 车牌颜色
        private String specialFlag;     // 特殊车辆类型
    }

    /**
     * 事件状态基类
     */
    public static abstract class BaseEventState {
        boolean isInEvent = false;
        String vehicleId;
        String plateNo;
        Integer carId;

        // 时间信息
        long startTime;
        long endTime;
        long lastSeenTime;

        // 位置信息
        double startMileage;
        double endMileage;
        String startStake;
        String endStake;
        int direction;
        int laneNo;
        float startLongitude;
        float startLatitude;
        float endLongitude;
        float endLatitude;

        // 车辆信息
        Integer vehicleType;
        Integer plateColor;
        String specialFlag;

        // 速度信息
        double minSpeed;
        double maxSpeed;
        double speedSum;
        double preSpeed;     // 事件前一帧速度
        double postSpeed;    // 事件后一帧速度
        int pointCount;

        // 轨迹点列表
        List<TrajectoryPoint> eventPoints;

        // 路段信息
        String waySectionId;
        String waySectionName;

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
            carId = null;
            startTime = 0;
            endTime = 0;
            lastSeenTime = 0;
            startMileage = 0;
            endMileage = 0;
            startStake = null;
            endStake = null;
            direction = -1;
            laneNo = -1;
            startLongitude = 0;
            startLatitude = 0;
            endLongitude = 0;
            endLatitude = 0;
            vehicleType = null;
            plateColor = null;
            specialFlag = null;
            minSpeed = 0;
            maxSpeed = 0;
            speedSum = 0;
            preSpeed = 0;
            postSpeed = 0;
            pointCount = 0;
            eventPoints = null;
            waySectionId = null;
            waySectionName = null;
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
            carId = null;
            startTime = 0;
            endTime = 0;
            lastSeenTime = 0;
            startMileage = 0;
            endMileage = 0;
            startStake = null;
            endStake = null;
            direction = -1;
            laneNo = -1;
            startLongitude = 0;
            startLatitude = 0;
            endLongitude = 0;
            endLatitude = 0;
            vehicleType = null;
            plateColor = null;
            specialFlag = null;
            minSpeed = 0;
            maxSpeed = 0;
            speedSum = 0;
            preSpeed = 0;
            postSpeed = 0;
            pointCount = 0;
            eventPoints = null;
            waySectionId = null;
            waySectionName = null;
        }
    }

    /**
     * 超速和低速检测处理器
     */
    public static class SpeedEventDetector extends RichFlatMapFunction<String, StandardEvent> {

        private transient ValueState<SpeedingEventState> speedingState;
        private transient ValueState<LowSpeedEventState> lowSpeedState;
        private transient ValueState<TrajectoryPoint> lastNormalPointState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<SpeedingEventState> speedingDescriptor =
                    new ValueStateDescriptor<>("speeding-state", SpeedingEventState.class);
            speedingState = getRuntimeContext().getState(speedingDescriptor);

            ValueStateDescriptor<LowSpeedEventState> lowSpeedDescriptor =
                    new ValueStateDescriptor<>("low-speed-state", LowSpeedEventState.class);
            lowSpeedState = getRuntimeContext().getState(lowSpeedDescriptor);

            ValueStateDescriptor<TrajectoryPoint> lastPointDescriptor =
                    new ValueStateDescriptor<>("last-point-state", TrajectoryPoint.class);
            lastNormalPointState = getRuntimeContext().getState(lastPointDescriptor);
        }

        @Override
        public void flatMap(String jsonString, Collector<StandardEvent> out) throws Exception {
            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                String timeStamp = jsonObject.optString("timeStamp", "");
                JSONArray pathList = jsonObject.optJSONArray("pathList");
                String waySectionId = jsonObject.optString("waySectionId", "");
                String waySectionName = jsonObject.optString("waySectionName", "");

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
                        processVehicleData(vehicleData, batchTimestamp, waySectionId, waySectionName, out);
                    }
                }

                checkTimeoutVehicles(batchTimestamp, out);

            } catch (Exception e) {
                System.err.println("速度事件检测处理异常: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private void processVehicleData(JSONObject vehicleData, long batchTimestamp,
                                        String waySectionId, String waySectionName,
                                        Collector<StandardEvent> out) throws Exception {
            Integer carId = vehicleData.optInt("id", -1);
            String vehicleId = String.valueOf(carId);
            String plateNo = vehicleData.optString("plateNo", "");
            double speed = vehicleData.optDouble("speed", 0.0);
            int direction = vehicleData.optInt("direction", -1);
            int laneNo = vehicleData.optInt("laneNo", -1);
            double mileage = vehicleData.optDouble("mileage", 0.0);
            float longitude = (float) vehicleData.optDouble("longitude", 0.0);
            float latitude = (float) vehicleData.optDouble("latitude", 0.0);
            String stakeId = vehicleData.optString("stakeId", "");
            Integer vehicleType = vehicleData.optInt("vehicleType", 0);
            Integer plateColor = vehicleData.optInt("plateColor", 0);
            String specialFlag = vehicleData.optString("specialFlag", "0");

            if (carId == -1) {
                return;
            }

            // 创建当前轨迹点
            TrajectoryPoint currentPoint = new TrajectoryPoint(
                    speed, mileage, laneNo, direction, batchTimestamp,
                    longitude, latitude, stakeId, vehicleType, plateColor, specialFlag
            );

            // 获取状态
            SpeedingEventState speedingStateValue = speedingState.value();
            LowSpeedEventState lowSpeedStateValue = lowSpeedState.value();
            if (speedingStateValue == null) {
                speedingStateValue = new SpeedingEventState();
            }
            if (lowSpeedStateValue == null) {
                lowSpeedStateValue = new LowSpeedEventState();
            }

            TrajectoryPoint lastNormalPoint = lastNormalPointState.value();

            // 更新最后可见时间和路段信息
            speedingStateValue.lastSeenTime = batchTimestamp;
            lowSpeedStateValue.lastSeenTime = batchTimestamp;
            speedingStateValue.waySectionId = waySectionId;
            speedingStateValue.waySectionName = waySectionName;
            lowSpeedStateValue.waySectionId = waySectionId;
            lowSpeedStateValue.waySectionName = waySectionName;

            // 检测超速和低速
            if (speed > SPEED_THRESHOLD) {
                handleSpeeding(carId, vehicleId, plateNo, currentPoint, lastNormalPoint, speedingStateValue);
                handleNormalSpeedForLowSpeed(carId, currentPoint, out, lowSpeedStateValue);
            } else if (speed < LOW_SPEED_THRESHOLD) {
                handleLowSpeed(carId, vehicleId, plateNo, currentPoint, lastNormalPoint, lowSpeedStateValue);
                handleNormalSpeedForSpeeding(carId, currentPoint, out, speedingStateValue);
            } else {
                handleNormalSpeedForSpeeding(carId, currentPoint, out, speedingStateValue);
                handleNormalSpeedForLowSpeed(carId, currentPoint, out, lowSpeedStateValue);
            }

            speedingState.update(speedingStateValue);
            lowSpeedState.update(lowSpeedStateValue);
            lastNormalPointState.update(currentPoint);
        }

        private void handleSpeeding(Integer carId, String vehicleId, String plateNo,
                                    TrajectoryPoint currentPoint, TrajectoryPoint lastNormalPoint,
                                    SpeedingEventState state) {
            if (!state.isInEvent) {
                System.out.println("检测到车辆超速开始: " + vehicleId + ", 速度: " + currentPoint.getSpeed() + "km/h");

                state.isInEvent = true;
                state.carId = carId;
                state.vehicleId = vehicleId;
                state.plateNo = plateNo;
                state.startTime = currentPoint.getTimestamp();
                state.endTime = currentPoint.getTimestamp();
                state.startMileage = currentPoint.getMileage();
                state.endMileage = currentPoint.getMileage();
                state.startStake = currentPoint.getStakeId();
                state.endStake = currentPoint.getStakeId();
                state.direction = currentPoint.getDirection();
                state.laneNo = currentPoint.getLaneNo();
                state.startLongitude = currentPoint.getLongitude();
                state.startLatitude = currentPoint.getLatitude();
                state.endLongitude = currentPoint.getLongitude();
                state.endLatitude = currentPoint.getLatitude();
                state.vehicleType = currentPoint.getVehicleType();
                state.plateColor = currentPoint.getPlateColor();
                state.specialFlag = currentPoint.getSpecialFlag();

                state.minSpeed = currentPoint.getSpeed();
                state.maxSpeed = currentPoint.getSpeed();
                state.speedSum = currentPoint.getSpeed();
                state.pointCount = 1;

                if (lastNormalPoint != null) {
                    state.preSpeed = lastNormalPoint.getSpeed();
                } else {
                    state.preSpeed = 0.0;
                }

                state.eventPoints = new ArrayList<>();
                state.eventPoints.add(currentPoint);

            } else {
                state.endTime = currentPoint.getTimestamp();
                state.endMileage = currentPoint.getMileage();
                state.endStake = currentPoint.getStakeId();
                state.endLongitude = currentPoint.getLongitude();
                state.endLatitude = currentPoint.getLatitude();

                state.minSpeed = Math.min(state.minSpeed, currentPoint.getSpeed());
                state.maxSpeed = Math.max(state.maxSpeed, currentPoint.getSpeed());
                state.speedSum += currentPoint.getSpeed();
                state.pointCount++;

                state.laneNo = currentPoint.getLaneNo();
                state.eventPoints.add(currentPoint);
            }
        }

        private void handleLowSpeed(Integer carId, String vehicleId, String plateNo,
                                    TrajectoryPoint currentPoint, TrajectoryPoint lastNormalPoint,
                                    LowSpeedEventState state) {
            if (!state.isInEvent) {
                System.out.println("检测到车辆低速开始: " + vehicleId + ", 速度: " + currentPoint.getSpeed() + "km/h");

                state.isInEvent = true;
                state.carId = carId;
                state.vehicleId = vehicleId;
                state.plateNo = plateNo;
                state.startTime = currentPoint.getTimestamp();
                state.endTime = currentPoint.getTimestamp();
                state.startMileage = currentPoint.getMileage();
                state.endMileage = currentPoint.getMileage();
                state.startStake = currentPoint.getStakeId();
                state.endStake = currentPoint.getStakeId();
                state.direction = currentPoint.getDirection();
                state.laneNo = currentPoint.getLaneNo();
                state.startLongitude = currentPoint.getLongitude();
                state.startLatitude = currentPoint.getLatitude();
                state.endLongitude = currentPoint.getLongitude();
                state.endLatitude = currentPoint.getLatitude();
                state.vehicleType = currentPoint.getVehicleType();
                state.plateColor = currentPoint.getPlateColor();
                state.specialFlag = currentPoint.getSpecialFlag();

                state.minSpeed = currentPoint.getSpeed();
                state.maxSpeed = currentPoint.getSpeed();
                state.speedSum = currentPoint.getSpeed();
                state.pointCount = 1;

                if (lastNormalPoint != null) {
                    state.preSpeed = lastNormalPoint.getSpeed();
                } else {
                    state.preSpeed = 0.0;
                }

                state.eventPoints = new ArrayList<>();
                state.eventPoints.add(currentPoint);

            } else {
                state.endTime = currentPoint.getTimestamp();
                state.endMileage = currentPoint.getMileage();
                state.endStake = currentPoint.getStakeId();
                state.endLongitude = currentPoint.getLongitude();
                state.endLatitude = currentPoint.getLatitude();

                state.minSpeed = Math.min(state.minSpeed, currentPoint.getSpeed());
                state.maxSpeed = Math.max(state.maxSpeed, currentPoint.getSpeed());
                state.speedSum += currentPoint.getSpeed();
                state.pointCount++;

                state.laneNo = currentPoint.getLaneNo();
                state.eventPoints.add(currentPoint);
            }
        }

        private void handleNormalSpeedForSpeeding(Integer carId, TrajectoryPoint currentPoint,
                                                  Collector<StandardEvent> out, SpeedingEventState state) throws Exception {
            if (state.isInEvent && state.carId != null && state.carId.equals(carId)) {
                state.postSpeed = currentPoint.getSpeed();

                long duration = state.endTime - state.startTime;
                if (duration >= MIN_EVENT_DURATION) {
                    StandardEvent event = createStandardEvent(state, EVENT_TYPE_SPEEDING, "超速行驶事件");
                    out.collect(event);
                    System.out.println("输出超速事件: " + event);
                } else {
                    System.out.println("超速持续时间不足，忽略: " + state.vehicleId + ", 持续时间: " + duration + "ms");
                }

                state.reset();
                speedingState.update(state);
            }
        }

        private void handleNormalSpeedForLowSpeed(Integer carId, TrajectoryPoint currentPoint,
                                                  Collector<StandardEvent> out, LowSpeedEventState state) throws Exception {
            if (state.isInEvent && state.carId != null && state.carId.equals(carId)) {
                state.postSpeed = currentPoint.getSpeed();

                long duration = state.endTime - state.startTime;
                if (duration >= MIN_EVENT_DURATION) {
                    StandardEvent event = createStandardEvent(state, EVENT_TYPE_LOW_SPEED, "低速行驶事件");
                    out.collect(event);
                    System.out.println("输出低速事件: " + event);
                } else {
                    System.out.println("低速持续时间不足，忽略: " + state.vehicleId + ", 持续时间: " + duration + "ms");
                }

                state.reset();
                lowSpeedState.update(state);
            }
        }

        private StandardEvent createStandardEvent(BaseEventState state, int eventType, String eventDescription) {
            StandardEvent event = new StandardEvent();

            // 生成事件ID和时间戳
            event.setEventId((int) EVENT_ID_GENERATOR.getAndIncrement());
            event.setTimeStamp(formatTimestamp(System.currentTimeMillis()));
            event.setEventType(eventType);

            // 位置信息
            event.setStartStake(state.startStake);
            event.setEndStake(state.endStake);
            event.setStartMileage((int) state.startMileage);
            event.setEndMilage((int) state.endMileage);
            event.setStartLongitude(state.startLongitude);
            event.setStartLatitude(state.startLatitude);
            event.setEndLongitude(state.endLongitude);
            event.setEndLatitude(state.endLatitude);

            // 事件详情
            event.setLaneNo(String.valueOf(state.laneNo));
            event.setDirection(state.direction);

            // 创建车辆列表
            JSONArray carList = new JSONArray();
            EventVehicle vehicle = new EventVehicle();
            vehicle.setCarId(state.carId);
            vehicle.setPlateNo(state.plateNo);
            vehicle.setPlateColor(state.plateColor);
            vehicle.setVehicleType(state.vehicleType);
            vehicle.setSpecialFlag(state.specialFlag);
            carList.put(vehicle.toJSONObject());
            event.setCarList(carList);

            // 附加信息
            event.setEventLevel(calculateEventLevel(state.maxSpeed, eventType));
            event.setEventDes(eventDescription);
            event.setEventReason("车辆速度异常");
            event.setEventSource(EVENT_SOURCE_VIDEO);
            event.setSourceRemark("视频分析自动检测");
            event.setWaySectionId(state.waySectionId);
            event.setWaySectionName(state.waySectionName);
            event.setManualAudit(true); // 超速和低速事件建议人工审核
            event.setConstructionVehicles(null);
            event.setConstructionPerson(null);

            return event;
        }

        private String calculateEventLevel(double maxSpeed, int eventType) {
            if (eventType == EVENT_TYPE_SPEEDING) {
                if (maxSpeed >= 140) return EVENT_LEVEL_EXTREME;
                else if (maxSpeed >= 120) return EVENT_LEVEL_MAJOR;
                else if (maxSpeed >= 110) return EVENT_LEVEL_LARGER;
                else return EVENT_LEVEL_GENERAL;
            } else if (eventType == EVENT_TYPE_LOW_SPEED) {
                if (maxSpeed <= 20) return EVENT_LEVEL_MAJOR;
                else if (maxSpeed <= 40) return EVENT_LEVEL_LARGER;
                else return EVENT_LEVEL_GENERAL;
            }
            return EVENT_LEVEL_GENERAL;
        }

        private void checkTimeoutVehicles(long currentTime, Collector<StandardEvent> out) throws Exception {
            // 超速事件超时检查
            SpeedingEventState speedingStateValue = speedingState.value();
            if (speedingStateValue != null && speedingStateValue.isInEvent) {
                long timeSinceLastUpdate = currentTime - speedingStateValue.lastSeenTime;
                if (timeSinceLastUpdate > 60000) {
                    System.out.println("清理超时超速事件: " + speedingStateValue.vehicleId);
                    if (speedingStateValue.pointCount > 0) {
                        long duration = speedingStateValue.endTime - speedingStateValue.startTime;
                        if (duration >= MIN_EVENT_DURATION) {
                            StandardEvent event = createStandardEvent(speedingStateValue, EVENT_TYPE_SPEEDING, "超速行驶事件(超时结束)");
                            out.collect(event);
                        }
                    }
                    speedingStateValue.reset();
                    speedingState.update(speedingStateValue);
                }
            }

            // 低速事件超时检查
            LowSpeedEventState lowSpeedStateValue = lowSpeedState.value();
            if (lowSpeedStateValue != null && lowSpeedStateValue.isInEvent) {
                long timeSinceLastUpdate = currentTime - lowSpeedStateValue.lastSeenTime;
                if (timeSinceLastUpdate > 60000) {
                    System.out.println("清理超时低速事件: " + lowSpeedStateValue.vehicleId);
                    if (lowSpeedStateValue.pointCount > 0) {
                        long duration = lowSpeedStateValue.endTime - lowSpeedStateValue.startTime;
                        if (duration >= MIN_EVENT_DURATION) {
                            StandardEvent event = createStandardEvent(lowSpeedStateValue, EVENT_TYPE_LOW_SPEED, "低速行驶事件(超时结束)");
                            out.collect(event);
                        }
                    }
                    lowSpeedStateValue.reset();
                    lowSpeedState.update(lowSpeedStateValue);
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

        private String formatTimestamp(long timestamp) {
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    java.time.Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS"));
        }
    }

    /**
     * 事件Kafka序列化器 - 符合标准事件格式
     */
    public static class EventKafkaSerializer implements KafkaRecordSerializationSchema<StandardEvent> {

        @Override
        public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) {
            // 初始化逻辑
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(StandardEvent event, KafkaSinkContext context, Long timestamp) {
            try {
                JSONObject json = new JSONObject();

                // 严格按照文档要求的字段顺序和名称
                json.put("eventId", event.getEventId());
                json.put("timeStamp", event.getTimeStamp());
                json.put("eventType", event.getEventType());
                json.put("startStake", event.getStartStake() != null ? event.getStartStake() : JSONObject.NULL);
                json.put("endStake", event.getEndStake() != null ? event.getEndStake() : JSONObject.NULL);
                json.put("startMileage", event.getStartMileage());
                json.put("endMilage", event.getEndMilage() != null ? event.getEndMilage() : JSONObject.NULL);
                json.put("startLongitude", event.getStartLongitude());
                json.put("startLatitude", event.getStartLatitude());
                json.put("endLongitude", event.getEndLongitude() != null ? event.getEndLongitude() : JSONObject.NULL);
                json.put("endLatitude", event.getEndLatitude() != null ? event.getEndLatitude() : JSONObject.NULL);
                json.put("laneNo", event.getLaneNo());
                json.put("direction", event.getDirection());
                json.put("carList", event.getCarList() != null ? event.getCarList() : new JSONArray());

                // 可选字段
                if (event.getEventLevel() != null) json.put("eventLevel", event.getEventLevel());
                if (event.getEventDes() != null) json.put("eventDes", event.getEventDes());
                if (event.getEventReason() != null) json.put("eventReason", event.getEventReason());
                if (event.getEventPicPath() != null) json.put("eventPicPath", event.getEventPicPath());
                if (event.getEventVideoPath() != null) json.put("eventVideoPath", event.getEventVideoPath());
                if (event.getEventSource() != null) json.put("eventSource", event.getEventSource());
                if (event.getSourceRemark() != null) json.put("sourceRemark", event.getSourceRemark());

                json.put("waySectionId", event.getWaySectionId());
                json.put("waySectionName", event.getWaySectionName());

                if (event.getManualAudit() != null) json.put("manualAudit", event.getManualAudit());
                if (event.getConstructionVehicles() != null) json.put("constructionVehicles", event.getConstructionVehicles());
                if (event.getConstructionPerson() != null) json.put("constructionPerson", event.getConstructionPerson());

                String topic = "wd.platform.specialEvent"; // 统一事件topic
                String jsonString = json.toString();

                return new ProducerRecord<>(
                        topic,
                        null,
                        System.currentTimeMillis(),
                        null,
                        jsonString.getBytes(StandardCharsets.UTF_8)
                );
            } catch (Exception e) {
                System.err.println("序列化标准事件失败: " + e.getMessage());
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

        List<String> topics = Arrays.asList("jtkj.jga.path");

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
    private static KafkaSink<StandardEvent> createKafkaSink() {
        String brokers = "10.48.53.82:9092";

        return KafkaSink.<StandardEvent>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(new EventKafkaSerializer())
                .build();
    }

    /**
     * 主方法 - 完整的Flink作业入口
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        System.out.println("启动超速和低速检测Flink作业...");

        // 1. 创建数据源
        DataStream<String> kafkaStream = createKafkaSource(env);

        // 2. 处理数据流
        SingleOutputStreamOperator<StandardEvent> speedEvents = kafkaStream
                .keyBy(data -> {
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
        KafkaSink<StandardEvent> kafkaSink = createKafkaSink();
        speedEvents.sinkTo(kafkaSink)
                .name("标准事件Kafka输出")
                .setParallelism(2);

        speedEvents.print()
                .name("标准事件控制台输出")
                .setParallelism(1);

        // 4. 执行作业
        System.out.println("开始执行Flink速度事件检测作业...");
        env.execute("实时车辆超速和低速检测作业");

        System.out.println("Flink作业执行完成");
    }
}