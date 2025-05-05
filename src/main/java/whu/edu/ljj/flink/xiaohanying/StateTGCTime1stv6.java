//package whu.edu.ljj.flink.xiaohanying;
//
//import com.alibaba.fastjson2.JSON;
//import com.alibaba.fastjson2.JSONObject;
//import javafx.util.Pair;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.common.state.StateTtlConfig;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.*;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
//import org.apache.flink.streaming.api.operators.ProcessOperator;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.clients.consumer.OffsetResetStrategy;
//import whu.edu.ljj.flink.utils.LocationOP;
//import whu.edu.ljj.flink.utils.myTools;
//
//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
//import static whu.edu.ljj.flink.xiaohanying.Utils.*;
//
//
///**
// * Special Edition for 孝汉应00
// * 测试其他版本的TrajectoryEnricher
// */
//public class StateTGCTime1stv6 {
//    // 京港澳公路有50个卡口，记得写静态代码块初始化GantryAssignment
//    private static GantryAssignment gantryAssign;
//
//    // 根据光栅数据事件格式确定参与计算的修正数
//    private static long d1TimeInterval;
//    private static boolean isD1TimeInitialized = false;
//    private static long d2TimeInterval;
//    private static boolean isD2TimeInitialized = false;
//
//
//    private static final int WINDOW_SIZE = 10;//用来预测的窗口大小
//    private static final Map<Long, PathPointData> PointMap = new ConcurrentHashMap<>();
//    static int timeout = 300;
//    static boolean firstEnter=true;
//    static Map<Long,Pair<Boolean,Integer>> nowMap= new ConcurrentHashMap<>();
//    //    carid  是否在路上  数据缺失了几次
//    static Map<Pair<Long,String>,String> zaMap= new ConcurrentHashMap<>();
//    // 京港澳公路有50个卡口，记得写静态代码块初始化GantryAssignment
//    public static void main(String[] args) throws Exception {
//        try {
//            gantryAssign = new GantryAssignment(args[0]);
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 京港澳公路有多段光栅，记得写自动创建ZMQ数据源的代码
//
//        // 创建 ZMQ 数据源
//        KeyedStream<TrajeData, Long> trajeStreamD1 = env.addSource(new ZmqSource("tcp://100.65.62.82:8030"))
//                .map(trajejson -> {
//                    TrajeData trajeData = JSON.parseObject(String.valueOf(trajejson), TrajeData.class);
//                    if(!isD1TimeInitialized) {
//                        d1TimeInterval = (200 - (trajeData.getTIME() % 1000 % 200)) % 200;
//                        System.out.println("d1TimeInterval："+d1TimeInterval);
//                        isD1TimeInitialized = true;
//                    }
//                    return trajeData;
//                })
//                .keyBy(trajeData -> trajeData.getTIME());
//
//        KeyedStream<TrajeData, Long> trajeStreamD2 = env.addSource(new ZmqSource("tcp://100.65.62.82:8040"))
//                .map(trajejson -> {
//                    TrajeData trajeData = JSON.parseObject(String.valueOf(trajejson), TrajeData.class);
//                    if(!isD2TimeInitialized) {
//                        d2TimeInterval = (200 - (trajeData.getTIME() % 1000 % 200)) % 200;
//                        System.out.println("d2TimeInterval："+d2TimeInterval);
//                        isD2TimeInitialized = true;
//                    }
//                    if(isD1TimeInitialized && d1TimeInterval != d2TimeInterval) {
//                        long fixTime = d2TimeInterval - d1TimeInterval;
//                        if(fixTime > 0)
//                            trajeData.setTIME(trajeData.getTIME() - (200 - fixTime));
//                        else
//                            trajeData.setTIME(trajeData.getTIME() + (200 + fixTime));
//                    }
//                    return trajeData;
//                })
//                .keyBy(trajeData -> trajeData.getTIME());
//
//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setTopics("rhy_iot_receive_lpr_bizAttr")
//                .setBootstrapServers("100.65.62.6:9092")
//                .setGroupId("gantry-group")
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
//                .setProperty("auto.offset.commit", "true")
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//
//        DataStreamSource<String> gantryStringSource = env.fromSource(kafkaSource,
//                WatermarkStrategy.noWatermarks(),
//                "Gantry Source");
//
//        ConnectedStreams<TrajeData, TrajeData> connectedStreams = trajeStreamD1.connect(trajeStreamD2);
//
//        // 使用 CoProcessFunction 进行匹配、合并和处理
//        DataStream<PathTData> mergedStream = connectedStreams
//                .process(new CoProcessFunction<TrajeData, TrajeData, PathTData>() {
//                    final StateTtlConfig ttlConfig = StateTtlConfig
//                            .newBuilder(Time.seconds(5)) // 设置状态存活时间为 5 秒
//                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 每次写入时更新存活时间
//                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期数据
//                            .build();
//
//                    // 用于存储来自流 A 的数据
//                    private ListState<TrajeData> trajeD1State;
//                    // 用于存储来自流 B 的数据
//                    private ListState<TrajeData> trajeD2State;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        // 初始化状态
//                        ListStateDescriptor<TrajeData> trajeD1Descriptor = new ListStateDescriptor<>("trajeD1State", TrajeData.class);
//                        trajeD1Descriptor.enableTimeToLive(ttlConfig);
//                        trajeD1State = getRuntimeContext().getListState(trajeD1Descriptor);
//                        ListStateDescriptor<TrajeData> trajeD2Descriptor = new ListStateDescriptor<>("trajeD2State", TrajeData.class);
//                        trajeD2Descriptor.enableTimeToLive(ttlConfig);
//                        trajeD2State = getRuntimeContext().getListState(trajeD2Descriptor);
//                    }
//
//                    @Override
//                    public void processElement1(TrajeData value, Context ctx, Collector<PathTData> out) throws Exception {
//                        // 将流 A 的数据添加到 ListState 中
//                        long receiveTime = System.currentTimeMillis();
////                        System.out.println("流A的数据：" + value + " 此刻的时间：" + receiveTime);
//
//                        trajeD1State.add(value);
//
//                        // 获取流 B 的 ListState 数据
//                        Iterable<TrajeData> bValues = trajeD2State.get();
//                        Iterator<TrajeData> bIterator = bValues.iterator();
//
//                        // 如果流 B 的数据存在，合并所有数据并输出，但实际上只有一个对应数据，这里只是解包的过程
//                        if (bIterator.hasNext()) {
//                            // 获取流 B 的所有数据
//                            List<PathPoint> bList = convertToPathList(bValues);
//
////                            System.out.println("对应流A的"+value+"数据的是流B的："+bList);
//                            // 获取流 A 的所有数据
//                            List<PathPoint> aList = convertToPathList(trajeD1State.get());
//
//                            // 合并并输出
//                            PathTData result = mergeData(aList, bList);
//                            out.collect(result);
//
//                            // 清空流 B 的 ListState
//                            trajeD2State.clear();
//                        }
//                    }
//
//                    @Override
//                    public void processElement2(TrajeData value, Context ctx, Collector<PathTData> out) throws Exception {
//                        // 将流 B 的数据添加到 ListState 中
//                        long receiveTime = System.currentTimeMillis();
////                        System.out.println("流B的数据：" + value + " 此刻的时间：" + receiveTime);
//
//                        trajeD2State.add(value);
//
//                        // 获取流 A 的 ListState 数据
//                        Iterable<TrajeData> aValues = trajeD1State.get();
//                        Iterator<TrajeData> aIterator = aValues.iterator();
//
//                        // 如果流 A 的数据存在，合并所有数据并输出
//                        if (aIterator.hasNext()) {
//                            // 获取流 B 的所有数据
//                            List<PathPoint> aList = convertToPathList(aValues);
////                            System.out.println("对应流B的" + value + "数据的是流A的：" + aList);
//                            // 获取流 A 的所有数据
//                            List<PathPoint> bList = convertToPathList(trajeD2State.get());
//                            // 合并并输出
//                            PathTData result = mergeData(aList, bList);
//                            out.collect(result);
//
//                            // 清空流 B 的 ListState
//                            trajeD1State.clear();
//                        }
//                    }
//
//                    // 合并逻辑
//                    private PathTData mergeData(List<PathPoint> d1List, List<PathPoint> d2List) {
//                        PathTData result = new PathTData();
//                        result.setTime(convertToTimestampMillis(d1List.get(0).getTimeStamp()));
//
//                        d1List.addAll(d2List);
//                        // 创建包含所有数据的列表
//                        List<PathPoint> allList = d1List;
//
//                        int sum = 0;
//
//                        // 过滤掉的原因是即使List是空的我也创建了一个空的PathPoint，这样可以保持处理一致
//                        for (int i = 0; i < allList.size(); ) {
//                            PathPoint pathPoint = allList.get(i);
//                            if (pathPoint.getId() == 0)
//                                allList.remove(i);
//                            else {
//                                i++;
//                                sum++;
//                            }
//                        }
//                        result.setPathNum(sum);
////                        System.out.println("这是allList的数据：" + allList);
//                        result.setPathList(allList);
//                        return result;
//                    }
//
//                    private List<PathPoint> convertToPathList(Iterable<TrajeData> pointValues) {
//                        List<PathPoint> pathList = new ArrayList<>();
//                        for (TrajeData tdata : pointValues) {
//                            List<TrajePoint> TDATA = tdata.getTDATA();
//                            long timeMillis = tdata.getTIME();
//                            if (TDATA.isEmpty()) {
//                                PathPoint PathPoint = new PathPoint();
//                                PathPoint.setTimeStamp(convertFromTimestampMillis(timeMillis));
//                                pathList.add(PathPoint);
//                            } else {
//                                for (TrajePoint point : TDATA) {
//                                    PathPoint ppoint = new PathPoint();
//                                    ppoint.setDirection(point.getDirect());
//                                    ppoint.setId(point.getID());
//                                    ppoint.setLaneNo(point.getWayno());
//                                    ppoint.setMileage(point.getTpointno());
//                                    ppoint.setPlateNo("");
//                                    ppoint.setSpeed(point.getSpeed());
//                                    ppoint.setTimeStamp(convertFromTimestampMillis(timeMillis));
//                                    pathList.add(ppoint);
//                                }
//                            }
//                        }
//                        return pathList;
//                    }
//                });
//
//
//        // 为合并后的光栅数据流添加键控
//        KeyedStream<PathTData, Long> keyedTrajeStream = mergedStream.keyBy(traje -> 1L);
//
//        KeyedStream<GantryData, Long> keyedGantryStream = gantryStringSource.transform("ParseJSON",
//                TypeInformation.of(GantryData.class),
//                new ProcessOperator<>(new ProcessFunction<String, GantryData>() {
//                    @Override
//                    public void processElement(String value, ProcessFunction<String, GantryData>.Context ctx, Collector<GantryData> out) throws Exception {
//                        JSONObject gantry = JSON.parseObject(value.replaceAll("^\\[", "").replaceAll("\\]$", ""));
//                        gantry.getJSONObject("params").remove("plateImage");
//                        gantry.getJSONObject("params").remove("headImage");
//
//                        // 初步筛选门架数据
//                        if (!gantry.getString("deviceId").equals("2714AC28-984A-42E9-A001-36395F243E99") &&
//                                !gantry.getString("deviceId").equals("A840C67A-6F9D-4EB8-A00C-44F73E540901"))
//                            return;
//
//                        if(gantry.getJSONObject("params").getString("envState").equals("99") &&
//                                gantry.getJSONObject("params").getJSONObject("tollRecord").getJSONArray("tollList").isEmpty())
//                            return;
////                        System.out.println("\n即将要参与匹配的gantryJSON："+gantry);
//
//                        GantryData gData = new GantryData();
//
//                        gData.setId(gantry.getString("deviceId"));
//                        gData.setDirection(gantryAssign.getGantriesByID().get(gantry.getString("deviceId")).getDirection());
//                        gData.setMileage(gantryAssign.getGantriesByID().get(gantry.getString("deviceId")).getMileage());
//                        gData.setUploadTime(gantry.getJSONObject("params").getString("uploadTime"));
//                        gData.setEnvState(gantry.getJSONObject("params").getString("envState"));
//                        gData.setPlateNumber(gantry.getJSONObject("params").getString("plateNumber"));
//                        if(!gantry.getJSONObject("params").getJSONObject("tollRecord").getJSONArray("tollList").isEmpty())
//                            gData.setTollPlateNumber(gantry.getJSONObject("params").getJSONObject("tollRecord").getJSONArray("tollList").getJSONObject(0).getString("tollPlateNumber"));
//                        if(!gData.getEnvState().equals("99"))
//                            gData.setHeadLaneCode(gantry.getJSONObject("params").getInteger("headLaneCode"));
//                        out.collect(gData);
//                    }
//                })
//        ).keyBy(gantry -> 1L);
//
//        SingleOutputStreamOperator<PathTData> pathTDataStream = keyedTrajeStream.connect(keyedGantryStream)
//                .process(new TrajectoryEnricherv6(d1TimeInterval, gantryAssign));
////        pathTDataStream.print();
//        SingleOutputStreamOperator<PathTData> endPathTDataStream = pathTDataStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {
//            @Override//5.56   33.76  86.64
//            public void flatMap(PathTData PathTData, Collector<PathTData> collector) throws Exception {
//                PathTData pathTData = new PathTData();
//                pathTData.setTime(PathTData.getTime());
//                pathTData.setTimeStamp(PathTData.getTimeStamp());
//                pathTData.setPathNum(PathTData.getPathNum());
//                pathTData.setWaySectionId(PathTData.getWaySectionId());
//                pathTData.setWaySectionName(PathTData.getWaySectionName());
//                List<PathPoint> list=new ArrayList<>();
//                //如果mergedata合法
//                if (PathTData.getPathList() != null) {
//
//                    //存车辆id对应的车辆是否在车道上、几次没有出现
//                    Map<Long, Pair<Boolean, Integer>> tempMap = new ConcurrentHashMap<>();
////                        b=true;
//                    updateMergePoint(PathTData);//更新当前车辆map
//                    List<PathPoint> p = PathTData.getPathList();
//                    for (PathPoint m : p) {
//                        if (firstEnter) {//第一次有数据，初始化nowmap
//                            FirstEnterputNowMap(PathTData);
//                            tempMap = nowMap;
//                        } else {
//                            tempMap.put(m.getId(), new Pair<>(true, 0));
//                        }
//                    }
//                    //不是第一次有数据，已经初始化nowmap，遍历nowmap，看是否有车没了,也就是nowmap有，目前没有
//                    for (Map.Entry<Long, Pair<Boolean, Integer>> entry : nowMap.entrySet()) {
//                        long key = entry.getKey();
//                        if (tempMap.get(key) == null) {//如果当前这辆车目前没有但是nowmap有，视为可能缺失
//                            if (nowMap.get(key).getKey()) {//==true就是还是三次以下 ==false就是已经消失三次以上，视为没了
//                                tempMap.put(key, new Pair<>(true, nowMap.get(key).getValue() + 1));
//                                System.out.println(tempMap.get(key).getValue());
//                                if (tempMap.get(key).getValue() == 1) {
//                                    tempMap.put(key, new Pair<>(false, nowMap.get(key).getValue()));
//                                    if (PointMap.get(key).getDirection() == 1 && PointMap.get(key).getMileage() > 15270) {
//                                        tempMap.remove(key);
//                                    }
//                                    if (PointMap.get(key).getDirection() == 2 && PointMap.get(key).getMileage() < 15690) {
//                                        tempMap.remove(key);
//                                    }
//                                    if (PointMap.get(key).getMileage() >= 1121970 && PointMap.get(key).getMileage() <= 1121990 && PointMap.get(key).getDirection() == 1) {
//                                        zaMap.put(new Pair<>(key, PointMap.get(key).getPlateNo()), "CK");
//                                    } else if (PointMap.get(key).getMileage() >= 1122544 && PointMap.get(key).getMileage() <= 1122564 && PointMap.get(key).getDirection() == 2) {
//                                        zaMap.put(new Pair<>(key, PointMap.get(key).getPlateNo()), "AK");
//                                    }
////                                        mark:移出zaMap的逻辑
//                                }
//                            } else tempMap.put(key, new Pair<>(false, nowMap.get(key).getValue() + 1));
//                        }
//                    }
//                    try (BufferedWriter writer = new BufferedWriter(new FileWriter("my.txt", true))) {
//                        //mark：用文件数据测试断连
//                        tempMap.forEach((key, value) -> {
//                                    if (!value.getKey()) {//不在路上
//                                        if (zaMap.get(key) != null) {//上了匝道
//                                            try {
//                                                PathPoint pathPoint = zhadaoPredictNextOne(PointMap.get(key));
//                                                list.add(pathPoint);
//                                                writer.write("[匝道预测]:" + myTools.PointToString(pathPoint));
//                                            } catch (IOException e) {
//                                                throw new RuntimeException(e);
//                                            }
//                                        } else {//没上匝道但数据断连14010
//                                            try {
//                                                PathPoint pathPoint = PredictNextOne(PointMap.get(key));
//                                                list.add(pathPoint);
//                                                writer.write("[主路预测]:" + myTools.PointToString(pathPoint));
//                                            } catch (IOException e) {
//                                                throw new RuntimeException(e);
//                                            }
//                                        }
//                                    } else {
//                                        PathPoint pathPoint = new PathPoint();
//                                        PathPointData pd= PointMap.get(key);
//                                        pathPoint.setMileage(pd.getMileage());
//                                        pathPoint.setId(pd.getId());
//                                        pathPoint.setSpeed(pd.getSpeed());
//                                        pathPoint.setDirection(pd.getDirection());
//                                        pathPoint.setLatitude(pd.getLatitude());
//                                        pathPoint.setLongitude(pd.getLongitude());
//                                        pathPoint.setLaneNo(pd.getLaneNo());
//                                        pathPoint.setCarAngle(pd.getCarAngle());
//                                        pathPoint.setOriginalColor(pd.getOriginalColor());
//                                        pathPoint.setPlateColor(pd.getPlateColor());
//                                        pathPoint.setStakeId(pd.getStakeId());
//                                        pathPoint.setPlateNo(pd.getPlateNo());
//                                        pathPoint.setOriginalType(pd.getOriginalType());
//                                        pathPoint.setVehicleType(pd.getVehicleType());
//                                        list.add(pathPoint);
//                                        myTools.printmergePointData(PointMap.get(key));
//                                        try {
//                                            writer.write(myTools.PointDataToString(PointMap.get(key)));
//                                        } catch (IOException e) {
//                                            throw new RuntimeException(e);
//                                        }
//                                    }
//                                    try {
//                                        writer.write(System.lineSeparator());
//                                    } catch (IOException e) {
//                                        throw new RuntimeException(e);
//                                    }
//                                }
//
//                        );
//                    }
//                    pathTData.setPathList(list);
//                    collector.collect(pathTData);
//                    //mark:防撞
//                    nowMap = tempMap;
//                }
//            }
//        });
//        env.execute("Plate Matching Flink Job");
//    }
//    //匝道上的预测
//    private static PathPoint zhadaoPredictNextOne(PathPointData data) throws IOException {
//        long currentTime = System.currentTimeMillis();
//        PathPoint p = null;
//        //经过的时间少于某一值（设置时间限制，避免一直计算预测）
//        if (currentTime-data.getLastReceivedTime()<80000000) {
//            // 使用车辆独立窗口计算
//            Float predictedSpeed = calculateMovingAverage(data.getSpeedWindow());
//            data.getSpeedWindow().addLast(predictedSpeed);
//            data.getSpeedWindow().removeFirst();
//            double distanceDiff = myTools.calculateDistance(predictedSpeed,250); // 米
//            double newTpointno=0;
//            if(data.getDirection()==1) newTpointno = data.getMileage() + distanceDiff; // 更新里程点
//            else newTpointno = data.getMileage() - distanceDiff; // 更新里程点
//            data.setMileage((int)newTpointno);
//            long carid = data.getId();
//            data.setSpeed(predictedSpeed);
//            data.setTimeStamp(myTools.toDateTimeString(currentTime));
//            //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
//            String sk=data.getStakeId();
//            //当前点的location
//            Location the= LocationOP.UseLLGetSK(data.getLatitude(),data.getLongitude(),zaMap.get(carid)).getKey();
//            //下一个点的location
//            Location sec=LocationOP.UseLLGetSK(data.getLatitude(),data.getLongitude(),zaMap.get(carid)).getValue();
//            if(sk.isEmpty())data.setStakeId(the.getLocation());
//            else{
//                data.setLatitude(Objects.requireNonNull(LocationOP.UseSKgetLL(sk, zaMap.get(carid))).getLatitude());
//                data.setLongitude(Objects.requireNonNull(LocationOP.UseSKgetLL(sk, zaMap.get(carid))).getLongitude());
//            }
//            double carangle=myTools.calculateBearing(data.getLatitude(),data.getLongitude(),sec.getLatitude(),sec.getLongitude());
//            //mark:==0还有问题
//            if(data.getCarAngle()==0)data.setCarAngle(carangle);
//            p=new PathPoint(data.getDirection(), data.getId(),data.getLaneNo(),(int) newTpointno, data.getPlateNo(), data.getSpeed(),data.getTimeStamp(), data.getPlateColor(), data.getVehicleType(),data.getLongitude(),data.getLatitude(),data.getCarAngle(),data.getStakeId(),  data.getOriginalType(), data.getPlateColor());
//            myTools.printmergePoint(p);
//        }
//        myTools.printmergePoint(p);
//        return p;
//    }
//    //主路上的预测
//    private static PathPoint PredictNextOne(PathPointData data) throws IOException {
//        long currentTime = System.currentTimeMillis();
//        Float predictedSpeed = calculateMovingAverage(data.getSpeedWindow());
//        data.getSpeedWindow().addLast(predictedSpeed);
//        data.getSpeedWindow().removeFirst();
//        //计算以预测速度跑了多远
//        double distanceDiff = myTools.calculateDistance(predictedSpeed,250); // 米
//        double newTpointno=0;
//        if(data.getDirection()==1) {
//            newTpointno = data.getMileage() + distanceDiff; // 更新里程点
//        }else {
//            newTpointno = data.getMileage() - distanceDiff; // 更新里程点
//        }
//        data.setMileage((int)newTpointno);
//        long carid = data.getId();
//        data.setSpeed(predictedSpeed);
//        data.setTimeStamp(myTools.toDateTimeString(currentTime));
//        //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
//        String sk=data.getStakeId();
//        //当前点的location
//        Location the=LocationOP.UseLLGetSK(data.getLatitude(),data.getLongitude(),"K").getKey();
//        //下一个点的location
//        Location sec=LocationOP.UseLLGetSK(data.getLatitude(),data.getLongitude(),"K").getValue();
//        if(sk.isEmpty())data.setStakeId(the.getLocation());
//        else{
//            data.setLatitude(Objects.requireNonNull(LocationOP.UseSKgetLL(sk, "K")).getLatitude());
//            data.setLongitude(Objects.requireNonNull(LocationOP.UseSKgetLL(sk, "K")).getLongitude());
//        }
////        mark:孝汉应==89，别的的话还得改
//        double carangle=89;
//        data.setCarAngle(carangle);
//        PathPoint p=new PathPoint(data.getDirection(), data.getId(),data.getLaneNo(),(int) newTpointno, data.getPlateNo(), data.getSpeed(),data.getTimeStamp(), data.getPlateColor(), data.getVehicleType(),data.getLongitude(),data.getLatitude(),data.getCarAngle(),data.getStakeId(),  data.getOriginalType(), data.getPlateColor());
////        myTools.printPathPoint(p);
//        return p;
//    }
//    private static PathPoint predicOne(PathPointData PathPointData){
//        return null;
//    }
//    //防撞，如果快碰到前车，则将其speedwindow全部-10
//    private static void deSpeedWindow(PathPointData PathPointData){
//        LinkedList<Float> speedWindow = PathPointData.getSpeedWindow();
//        ListIterator<Float> iterator = speedWindow.listIterator();
//        while (iterator.hasNext()) {
//            float originalValue = iterator.next();
//            iterator.set(originalValue - 10);
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
//    private static void FirstEnterputNowMap(PathTData PathTData) throws IOException {
//        List<PathPoint> p=PathTData.getPathList();
//        for(PathPoint m:p){
//            nowMap.put(m.getId(),new Pair<>(true,0));
//        }
//    }
//    private static void updateMergePoint(PathTData PathTData) {
//        for (PathPoint Point : PathTData.getPathList()) {//遍历当前这条TrajeData消息中的所有TrajePoint
//            PathPointData data = PointMap.compute(Point.getId(), (k, v) -> new PathPointData());
//            synchronized (data) {
//                data.getSpeedWindow().add(Point.getSpeed());
//                if (data.getSpeedWindow().size() > WINDOW_SIZE) {
//                    data.getSpeedWindow().removeFirst();
//                }
//                data.setMileage(Point.getMileage());
//                data.setId(Point.getId());
//                data.setSpeed(Point.getSpeed());
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