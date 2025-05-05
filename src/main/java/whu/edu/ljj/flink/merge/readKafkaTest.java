//package whu.edu.ljj.flink.merge;
//import javafx.util.Pair;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple0;
//import whu.edu.ljj.flink.utils.myTools;
//import com.alibaba.fastjson2.JSON;
//import com.alibaba.fastjson2.JSONObject;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import whu.edu.ljj.demos.ZmqSource;
//import whu.edu.ljj.flink.utils.LocationOP;
//import static whu.edu.ljj.flink.xiaohanying.Utils.*;
//import org.apache.flink.util.Collector;
//
//import java.io.IOException;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
//public class readKafkaTest {
//    private static final int WINDOW_SIZE = 10;//用来预测的窗口大小
//    private static final Map<Integer, PathPointData> PointMap = new ConcurrentHashMap<>();
//    static int timeout = 300;
//    private static final int TIMEOUT_MS = 2000; // 超时时间
//    static boolean a=true;
//    static boolean b=true;
//    static boolean firstEnter=true;
//    static long carid;
//    static  List<Integer> zhadaoMileages=new ArrayList<>();
//    static Map<Pair<Integer,Long>,Boolean> IfLaterMap= new ConcurrentHashMap<>();
//    //           carid  zhadaoMileages    是否有数据
//    static Map<Integer,Boolean> nowMap= new ConcurrentHashMap<>();
//    static Map<Integer,Boolean> laterMap= new ConcurrentHashMap<>();
//    //        carid  此时路上是否有这辆车
//    public static void main(String[] args) throws Exception {
//        zhadaoMileages.add(10000);
//
//        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
//
//            // 配置Kafka连接信息
//            String brokers = "100.65.38.40:9092";
//            String topic = "MergedPathData";
//            String groupId = "flink_consumer_group";
//
//            // 创建Kafka数据源
//            KafkaSource<String> source = KafkaSource.<String>builder()
//                    .setBootstrapServers(brokers)
//                    .setTopics(topic)
//                    .setGroupId(groupId)
//                    .setStartingOffsets(OffsetsInitializer.latest())
//                    .setValueOnlyDeserializer(new SimpleStringSchema())
//                    .build();
//
//            // 从Kafka读取数据
//            DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//            DataStream<PathTData> parsedStream = kafkaStream
//                    .flatMap((String jsonStr, Collector<PathTData> out) -> {
//                        try {
//                            PathTData data = JSON.parseObject(jsonStr, PathTData.class);
//                            out.collect(data);
//                        } catch (Exception e) {
//                            System.err.println("JSON解析失败: " + jsonStr);
//                        }
//                    }).returns(PathTData.class).keyBy(mergeData -> mergeData.getTime());
//            parsedStream.flatMap(new FlatMapFunction<PathTData, Object>() {
//                @Override//5.56   33.76  86.64
//                public void flatMap(PathTData PathTData, Collector<Object> collector) throws Exception {
//                    if (PathTData.getPathList() != null && !PathTData.getPathList().isEmpty()) {//如果mergedata合法
//                        Map<Integer,Boolean> tempMap= new ConcurrentHashMap<>();
////                        b=true;
//                        updateMergePoint(PathTData);//更新当前车辆map
//                        List<PathPoint> p=PathTData.getPathList();
//                        for(PathPoint m:p){
//                            if(firstEnter){//第一次有数据，初始化nowmap
//                                FirstEnterputNowMap(PathTData);
//                                tempMap=nowMap;
//                            }else {
//                                tempMap.put(m.getId(),true);
//                            }
//                            for(long mi:zhadaoMileages){//对于所有的匝道里程，看车是否在匝道周围并且后面没有数据了
//                                //不是第一条数据并且判断是否上匝道
//                                if(m.getMileage()>=mi-1000&&m.getMileage()<=mi+1000||!nowMap.get(m.getId())){//在匝道附近或者认为已上车
//                                    //不是第一次有数据，已经初始化nowmap，遍历nowmap，看是否有车没了,也就是nowmap有，目前没有
//                                    for (Map.Entry<Integer, Boolean> entry : nowMap.entrySet()) {
//                                        int key = entry.getKey();
//                                        Boolean value = entry.getValue();
//                                        // 处理键值对
//                                        if(value &&tempMap.get(key)==null){//即后面没有数据，认为上了匝道
//                                            zhadaoPredictOrSOUTDirect(m);
//                                        }
//                                    }
////                                    Set<K> resultKeys = new HashSet<>();
////                                    for (K key : nowMap.keySet()) {
////                                        // 检查 nowMap 的 value 是否为 true，且 tempMap 中存在该 key 且 value 为 false
////                                        if (Boolean.TRUE.equals(nowMap.get(key))
////                                                && Boolean.FALSE.equals(tempMap.get(key))) {
////                                            resultKeys.add(key);
////                                        }
////                                    }
//                                }
//
//                                //更新nowmap
//                                nowMap=tempMap;
//                            }
//                            WhetherPredictOrSOUTDirect(PathTData);
//                        }
//                    }
//                }
//            });
//            // 执行任务
//            env.execute("Flink Read Kafka");
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//    private static void FirstEnterputNowMap(PathTData PathTData) throws IOException {
//        List<PathPoint> p=PathTData.getPathList();
//        for(PathPoint m:p){
//            nowMap.put(m.getId(),true);
//        }
//    }
//    private static void WhetherPredictOrSOUTDirect(PathTData PathTData) throws IOException {
//        long currentTime = System.currentTimeMillis();
//        for (Map.Entry<Integer, PathPointData> entry : PointMap.entrySet()) {
//            PathPointData data = entry.getValue();
//            synchronized (data) {
//                //如果很久没有收到数据
//                if (currentTime - PathTData.getTime() >= TIMEOUT_MS && !data.getSpeedWindow().isEmpty()) {
//
//                    // 使用车辆独立窗口计算
//                    Float predictedSpeed = calculateMovingAverage(data.getSpeedWindow());
//                    data.getSpeedWindow().addLast(predictedSpeed);
//                    data.getSpeedWindow().removeFirst();
//                    int distanceDiff = (int) (predictedSpeed * 0.2); // 米
//                    int newTpointno=0;
//                    if(data.getDirection()==1) {
//                        newTpointno = data.getMileage() + distanceDiff; // 更新里程点
//                    }else {
//                        newTpointno = data.getMileage() - distanceDiff; // 更新里程点
//                    }
//                    data.setMileage(newTpointno);
//                    long carid = data.getId();
//                    int carType = data.getVehicleType();
//                    // 输出预测结果
//                    Location location1 = LocationOP.getLocation("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\data\\孝汉应.json", newTpointno);
//                    Location location2 = LocationOP.getLocation("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\data\\孝汉应.json", newTpointno + (int) (calculateMovingAverage(data.getSpeedWindow()) * 0.2));
//                    String output = String.format(
//"[预测]timestamp: %s  车辆ID:%d  车牌号:%s    车型:%d  车辆颜色:%d  速度:%.10f km/h  坐标: %.6f,%.6f  正北航向角：%.3f    车道号:%d  里程:%d   方向:%d (基于最近%d条数据)",
//data.getTimeStamp(), carid, data.getPlateNo(), carType,data.getPlateColor(), predictedSpeed, location1.getLatitude(), location1.getLongitude(), myTools.calculateBearing(location1.getLatitude(), location1.getLongitude(), location2.getLatitude(), location2.getLongitude()), data.getLaneNo(), newTpointno, data.getDirection(), data.getSpeedWindow().size()
//                    );
//                    System.out.println(output);
//                }else {
////                    if(b) {myTools.getCaridXXXFromMergeData(PathTData,carid);b=false;}
//                        myTools.printmergePointData(data);
//                }
//            }
//        }
//    }
//    private static void zhadaoPredictOrSOUTDirect(PathPoint PathPoint) throws IOException {
//        long currentTime = System.currentTimeMillis();
//            PathPointData data = PointMap.get(PathPoint.getId());
//            synchronized (data) {
//                //经过的时间少于某一值（设置时间限制，避免一直计算预测）
//                if (currentTime-data.getLastReceivedTime()<80000000) {
//
//                    // 使用车辆独立窗口计算
//                    Float predictedSpeed = calculateMovingAverage(data.getSpeedWindow());
//                    data.getSpeedWindow().addLast(predictedSpeed);
//                    data.getSpeedWindow().removeFirst();
//                    double distanceDiff = predictedSpeed * 0.2; // 米
//                    double newTpointno=0;
//                    if(data.getDirection()==1) {
//                        newTpointno = data.getMileage() + distanceDiff; // 更新里程点
//                    }else {
//                        newTpointno = data.getMileage() - distanceDiff; // 更新里程点
//                    }
//                    data.setMileage((int)newTpointno);
//                    long carid = data.getId();
//                    int carType = data.getVehicleType();
//                    // 输出预测结果
//                    Location location1 = LocationOP.getLocation("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\data\\孝汉应.json", newTpointno);
//                    Location location2 = LocationOP.getLocation("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\data\\孝汉应.json", newTpointno + (int) (calculateMovingAverage(data.getSpeedWindow()) * 0.2));
//                    String output = String.format(
//                            "[预测]timestamp: %s  车辆ID:%d  车牌号:%s    车型:%d  车辆颜色:%d  速度:%.10f km/h  坐标: %.6f,%.6f  正北航向角：%.3f    车道号:%d  里程:%d   方向:%d (基于最近%d条数据)",
//                            data.getTimeStamp(), carid, data.getPlateNo(), carType,data.getPlateColor(), predictedSpeed, location1.getLatitude(), location1.getLongitude(), myTools.calculateBearing(location1.getLatitude(), location1.getLongitude(), location2.getLatitude(), location2.getLongitude()), data.getLaneNo(), newTpointno, data.getDirection(), data.getSpeedWindow().size()
//                    );
//                    System.out.println(output);
//                    }
//        }
//    }
//    private static float calculateMovingAverage(LinkedList<Float> speedWindow) {
//        synchronized (speedWindow) {
//            return (float) speedWindow.stream()
//                    .mapToDouble(Float::doubleValue)
//                    .average()
//                    .orElse(Double.NaN);
//        }
//    }
//    private static void updateMergePoint(PathTData PathTData) {
//        for (PathPoint Point : PathTData.getPathList()) {//遍历当前这条TrajeData消息中的所有TrajePoint
//            PathPointData data = PointMap.compute(Point.getId(), (k,v) -> new PathPointData());
//            synchronized (data) {
//                data.getSpeedWindow().add(Point.getSpeed());
//                if (data.getSpeedWindow().size() > WINDOW_SIZE) {
//                    data.getSpeedWindow().removeFirst();
//                }
//                data.setMileage(Point.getMileage());
//                data.setId(Point.getId());
//                data.setDirection(Point.getDirection());
//                data.setLatitude(Point.getLatitude());
//                data.setLongitude(Point.getLongitude());
//                data.setLaneNo(Point.getLaneNo());
//                data.setCarAngle(Point.getCarAngle());
//                data.setOriginalColor(Point.getOriginalColor());
//                data.setPlateColor(Point.getPlateColor());
//                data.setStakeId(Point.getStakeId());
//                data.setPlateNo(Point.getPlateNo());
//                data.setOriginalType(Point.getOriginalType());
//                data.setLastReceivedTime(PathTData.getTime());
//                data.setTimeStamp(Point.getTimeStamp());
//                data.setVehicleType(Point.getVehicleType());
//            }
//        }
//    }
//}
