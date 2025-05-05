package whu.edu.moniData;

import com.alibaba.fastjson2.JSON;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import whu.edu.ljj.flink.utils.myTools;
import whu.edu.ljj.flink.xiaohanying.Utils.PathPoint;
import whu.edu.ljj.flink.xiaohanying.Utils.PathPointData;
import whu.edu.ljj.flink.xiaohanying.Utils.PathTData;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class DealWithJiZhan {
    private static final int WINDOW_SIZE = 20;//用来预测的窗口大小
    private static final Map<Long, PathPointData> pointMap = new ConcurrentHashMap<>();
    private static final Map<Long, PathPointData> JizhanPointMap = new ConcurrentHashMap<>();//雷视数据获取到的匝道上的所有车
    static boolean firstEnter = true;
    static Map<Long, PathPoint> lastMap = new ConcurrentHashMap<>();
    static Map<Long, PathPoint> tempMap = new ConcurrentHashMap<>();

    //    carid  是否在路上  数据缺失了几次
    static Map<Pair<Long, String>, String> zaMap = new ConcurrentHashMap<>();
    //       carid  carNumber  匝道编号
    private static final long mainRoadMinMillage = 0;//主路上的最小里程
    private static final long mainRoadMaxMillage = 1111111111;//主路上的最大里程
    private static String pathTimeStamp = "";
    private static float predictedSpeed = 0;//预测速度
    private static double distanceDiff = 0;
    private static long pathTime = 0;
    private static int tcount = 0;
    private static long t1 = 0;
    private static boolean tb1 = true;
    private static boolean tb2 = true;
    private static long t2 = 0;
    private static long t3 = 0;
    private static long temp = 0;
    private static long dis = 0;
    private static int newscount = 0;
    private static TrafficEventUtils.MileageConverter mileageConverter1;
    private static TrafficEventUtils.MileageConverter mileageConverter2;
    private static TrafficEventUtils.StakeAssignment stakeAssign1;
    private static TrafficEventUtils.StakeAssignment stakeAssign2;

    static {
        try {
            mileageConverter1 = new TrafficEventUtils.MileageConverter("sx_json.json");
            mileageConverter2 = new TrafficEventUtils.MileageConverter("xx_json.json");
            stakeAssign1 = new TrafficEventUtils.StakeAssignment("sx_json.json");
            stakeAssign2 = new TrafficEventUtils.StakeAssignment("xx_json.json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(3);
            // 配置Kafka连接信息
//            String brokers = "100.65.38.139:9092";
//            String groupId = "flink_consumer_group";
//            List<String> topics = Arrays.asList("fiberDataTest1", "fiberDataTest2", "fiberDataTest3");
            String brokers = args[0];
            String groupId = "flink_consumer_group";
            List<String> topics  = Arrays.asList(Arrays.copyOfRange(args, 1, args.length));;
//            List<String> topics = Collections.singletonList("news-topic");
//            List<String> topics = Collections.singletonList("MergedPathData.sceneTest.1");
            // 创建Kafka数据源
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperty("message.max.bytes", "16777216")
                    .setProperty("max.partition.fetch.bytes", "16777216")
                    .build();

            // 从Kafka读取数据
            DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source1");
            DataStream<PathTData> parsedStream = kafkaStream
                    .flatMap((String jsonStr, Collector<PathTData> out) -> {
                        try {
                            PathTData data = null;
                            System.out.println(jsonStr);
//                            System.out.println();
                            //验证，如果json的前几位是timestamp，则认为是mergedata
                            data = JSON.parseObject(jsonStr, PathTData.class);
                            out.collect(data);
//                            }
                        } catch (Exception e) {
                            System.err.println("JSON解析失败: " + jsonStr);
                        }
                    }).returns(PathTData.class).keyBy(PathTData::getTime);
            // 3. 写入 Kafka Sink
            KafkaSink<String> dealStreamSink = KafkaSink.<String>builder()
                    .setBootstrapServers("100.65.38.139:9092")
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.builder()
                                    .setTopic("completed.pathdata")
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build()
                    )
                    .setProperty("max.request.size", "629145600") // 20MB
                    .setProperty("message.max.bytes", "629145600") // Kafka Broker 的 message.max.bytes
                    .build();

            DataStream<String> jsonStream = parsedStream
                    .map(JSON::toJSONString)
                    .returns(String.class);

            jsonStream.sinkTo(dealStreamSink);

            env.execute("Flink completion");

        }//flink env

    }//main



    private static PathPoint predictMainRoadNextOne_UpdataPointMap(long key) {
        PathPointData data=pointMap.get(key);
        LinkedList<Float> spw=data.getSpeedWindow();
        predictedSpeed=calculateMovingAverage(spw);
        spw.addLast(predictedSpeed);
        if(spw.size()>WINDOW_SIZE)spw.removeFirst();
        double newTpointno=0;
        distanceDiff = myTools.calculateDistance(predictedSpeed, 200);
        if(data.getDirection()==1) {
            newTpointno = data.getMileage() + distanceDiff; // 更新里程点
        }else {
            newTpointno = data.getMileage() - distanceDiff; // 更新里程点
        }
        if(newTpointno<mainRoadMinMillage||newTpointno>mainRoadMaxMillage)return null;
        String stake=data.getStakeId();
        String newStake=MileageToStake((int)(stakeToMileage(stake)+distanceDiff));
        TrafficEventUtils.MileageConverter converter = (data.getDirection() == 1) ? mileageConverter1 : mileageConverter2;
        double[] d=converter.findCoordinate(stakeToMileage(newStake)).getLnglat();
        //问题；角度
        double carangle=89;
        data.setCarAngle(carangle);
        data.setMileage((int)newTpointno);
        data.setSpeed(predictedSpeed);
//        data.setTimeStamp(pathTimeStamp);//未接收到，不更新
        data.setLatitude(d[1]);
        data.setLongitude(d[0]);
        data.setSpeedWindow(spw);
        data.setStakeId(newStake);
        pointMap.put(key, data);
        PathPoint pp=PDToPP(data);
        myTools.printmergePoint(pp);
        return pp;
    }
    private static int stakeToMileage(String stakeId) {
        return Integer.parseInt(stakeId.split("\\+")[0].substring(1)) * 1000 + Integer.parseInt(stakeId.split("\\+")[1]);
    }
    private static String MileageToStake(int newMileage) {
        return newMileage/1000+"+"+(newMileage-(newMileage/1000*1000));
    }
    private static float calculateMovingAverage(LinkedList<Float> speedWindow) {
        return (float) speedWindow.stream()
                .mapToDouble(Float::doubleValue)
                .average()
                .orElse(Double.NaN);
    }
    private static void putNowDataIntoTempMap(PathTData pathTData){
        List<PathPoint> p=pathTData.getPathList();
        for(PathPoint m:p) tempMap.put(m.getId(),m);

    }

    private static void firstEnterInitializePointMapAndlastMap (PathTData pathTData){
        List<PathPoint> p=pathTData.getPathList();
        for(PathPoint m:p){
            lastMap.put(m.getId(),m);
            PathPointData pp=PPToPD(m);
            pp.setLastReceivedTime(pathTData.getTime());
            pp.getSpeedWindow().add(m.getSpeed());
            pointMap.put(m.getId(),pp);
        }
    }
    private static void firstEnterInsertPointMap(PathPoint p){
            PathPointData pp=PPToPD(p);
            //temp是当前时间
            pp.setLastReceivedTime(temp);
            pp.getSpeedWindow().add(p.getSpeed());
            pointMap.put(p.getId(),pp);

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
}//public buquan class