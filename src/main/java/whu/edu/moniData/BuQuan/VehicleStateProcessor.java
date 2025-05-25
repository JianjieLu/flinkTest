package whu.edu.moniData.BuQuan;
import com.alibaba.fastjson2.JSON;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import whu.edu.ljj.flink.utils.JsonReader;
import whu.edu.ljj.flink.utils.myTools;
import whu.edu.ljj.flink.xiaohanying.Utils.*;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static whu.edu.ljj.flink.utils.LocationOP.UseSKgetLL;
import static whu.edu.ljj.flink.utils.calAngle.calculateBearing;

public class VehicleStateProcessor extends KeyedProcessFunction<Long, PathPoint, PathPoint> {

    private transient MapState<Long, PathPointData> vehicleState;

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<Long, PathPointData> descriptor = new MapStateDescriptor<>(
                "vehicleState",
                Long.class,
                PathPointData.class
        );
        // 设置状态 TTL 避免无限增长
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
        descriptor.enableTimeToLive(ttlConfig);

        vehicleState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(PathPoint point, Context ctx, Collector<PathPoint> out) throws Exception {
        // 更新当前车辆状态
        PathPointData currentData = vehicleState.get(point.getId());
        if (currentData == null) {
            currentData = PPToPD(point);
            currentData.setSpeedWindow(new LinkedList<>());
        }
        updateState(currentData, point);
        vehicleState.put(point.getId(), currentData);

        // 注册下次补全的定时器（例如 1 秒后）
        ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 1000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PathPoint> out) throws Exception {
        PathPointData data = vehicleState.get(ctx.getCurrentKey());
        if (data != null) {
            // 生成预测数据并输出
            PathPoint predicted = predict(data);
            out.collect(predicted);
            // 更新状态
            vehicleState.put(ctx.getCurrentKey(), PPToPD(predicted));
        }
    }

    private void updateState(PathPointData data, PathPoint newPoint) {
        // 根据新数据更新状态的逻辑
        data.setSpeed(newPoint.getSpeed());
        data.setMileage(newPoint.getMileage());
        // ... 其他字段更新
    }
    private static PathPoint PDToPP(PathPointData Point) {
        PathPoint pathPoint = new PathPoint();

        pathPoint.setMileage(Point.getMileage());
        pathPoint.setId(Point.getId());
        pathPoint.setSpeed(Point.getSpeed());
        pathPoint.setDirection(Point.getDirection());
        pathPoint.setLatitude(Point.getLatitude());
        pathPoint.setLongitude(Point.getLongitude());
        pathPoint.setLaneNo(Point.getLaneNo());
        pathPoint.setCarAngle(Point.getCarAngle());
        pathPoint.setOriginalColor(Point.getOriginalColor());
        pathPoint.setPlateColor(Point.getPlateColor());
        pathPoint.setStakeId(Point.getStakeId());
        pathPoint.setPlateNo(Point.getPlateNo());
        pathPoint.setOriginalType(Point.getOriginalType());
        pathPoint.setVehicleType(Point.getVehicleType());
        pathPoint.setTimeStamp(Point.getTimeStamp());
        return pathPoint;
    }
    private static PathPointData PPToPD(PathPoint Point) {
        PathPointData pathPoint = new PathPointData();
        pathPoint.setMileage(Point.getMileage());
        pathPoint.setId(Point.getId());
        pathPoint.setSpeed(Point.getSpeed());
        pathPoint.setDirection(Point.getDirection());
        pathPoint.setLatitude(Point.getLatitude());
        pathPoint.setLongitude(Point.getLongitude());
        pathPoint.setLaneNo(Point.getLaneNo());
        pathPoint.setCarAngle(Point.getCarAngle());
        pathPoint.setOriginalColor(Point.getOriginalColor());
        pathPoint.setPlateColor(Point.getPlateColor());
        pathPoint.setStakeId(Point.getStakeId());
        pathPoint.setPlateNo(Point.getPlateNo());
        pathPoint.setOriginalType(Point.getOriginalType());
        pathPoint.setVehicleType(Point.getVehicleType());
        pathPoint.setTimeStamp(Point.getTimeStamp());
        pathPoint.setSpeedWindow(new LinkedList<>());

        return pathPoint;
    }
    private PathPoint predict(PathPointData data) {
        // 实现预测逻辑
        return PDToPP(data);
    }
}