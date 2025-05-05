//package whu.edu.ljj.flink;
//
//import com.alibaba.fastjson2.JSON;
//import com.alibaba.fastjson2.JSONObject;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.*;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.ConnectedStreams;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
//import org.apache.flink.streaming.api.operators.ProcessOperator;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.clients.consumer.OffsetResetStrategy;
//
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
//import whu.edu.ljj.flink.utils.LocationOP;
//import static whu.edu.ljj.flink.xiaohanying.Utils.*;
//import whu.edu.ljj.flink.utils.myTools;
//
//import static whu.edu.ljj.flink.utils.Utils.*;
//
//
//public class StateTGCTimeFirst {
//    private static final int WINDOW_SIZE = 10;//用来预测的窗口大小
//    private static final Map<String, PathPointData> PointMap = new ConcurrentHashMap<>();
//    static int timeout = 300;
//    private static final int TIMEOUT_MS = 400; // 超时时间
//    static boolean firstEnter=true;
//    static  long zadaoMileage;
//    static Map<javafx.util.Pair<Long,Long>,Boolean> IfLaterMap= new ConcurrentHashMap<>();
//    //           carid  zhadaoMileages    是否有数据
//    static Map<String, javafx.util.Pair<Boolean,Integer>> nowMap= new ConcurrentHashMap<>();
//    //    carid  是否在路上  数据缺失了几次
//    static Map<String,String> zaMap= new ConcurrentHashMap<>();
//    //       carid  匝道编号
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
//        //真实情况肯定有很多段光栅，记得写自动创建ZMQ数据源的代码
//
//        // 创建 ZMQ 数据源
//        KeyedStream<TrajeData, Long> trajeStreamD1 = env.addSource(new ZmqSource("tcp://100.65.62.82:8030"))
//                .map(trajeData -> JSON.parseObject(String.valueOf(trajeData), TrajeData.class))
//                .keyBy(trajeData -> trajeData.getTIME());
//
//        KeyedStream<TrajeData, Long> trajeStreamD2 = env.addSource(new ZmqSource("tcp://100.65.62.82:8040"))
//                .map(trajeData -> JSON.parseObject(String.valueOf(trajeData), TrajeData.class))
//                .keyBy(trajeData -> trajeData.getTIME());
//
//        KeyedStream<TrajeData, Long> trajeStreamD3 = env.addSource(new ZmqSource("tcp://100.65.62.82:8040"))
//                .map(trajeData -> JSON.parseObject(String.valueOf(trajeData), TrajeData.class))
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
//                        for (int i = 0; i < allList.size(); ) {
//                            PathPoint PathPoint = allList.get(i);
//                            if (PathPoint.getId() == null)
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
//                                PathPoint.setTimestamp(convertFromTimestampMillis(timeMillis));
//                                pathList.add(PathPoint);
//                            } else {
//                                for (TrajePoint point : TDATA) {
//                                    PathPoint ppoint = new PathPoint();
//                                    ppoint.setDirection(point.getDirect());
//                                    ppoint.setId(String.valueOf(point.getID()));
//                                    ppoint.setLaneNo(point.getWayno());
//                                    ppoint.setMileage(point.getTpointno());
//                                    ppoint.setPlateNumber("");
//                                    ppoint.setSpeed(point.getSpeed());
//                                    ppoint.setTimestamp(convertFromTimestampMillis(timeMillis));
//                                    pathList.add(ppoint);
//                                }
//                            }
//                        }
//                        return pathList;
//                    }
//                });
//
//        mergedStream.flatMap(new FlatMapFunction<PathTData, Object>() {
//            @Override//5.56   33.76  86.64
//            public void flatMap(PathTData pathTData, Collector<Object> collector) throws Exception {
//                if (pathTData.getPathList() != null && !pathTData.getPathList().isEmpty()) {//如果PathTData合法
//                    Map<String, javafx.util.Pair<Boolean,Integer>> tempMap= new ConcurrentHashMap<>();
////                        b=true;
//                    updatePathTData(pathTData);//更新当前车辆map
//                    List<PathPoint> p=pathTData.getPathList();
//                    for(PathPoint m:p){
//
//                        if(firstEnter){//第一次有数据，初始化nowmap
//                            FirstEnterputNowMap(pathTData);
//                            tempMap=nowMap;
//                        }else {
//                            tempMap.put(m.getId(),new javafx.util.Pair<>(true,0));
//                        }
//                    }
////                        //不是第一条数据并且判断是否上匝道
////                        if(m.getMileage()>=zadaoMileage-1000&&m.getMileage()<=zadaoMileage+1000||!nowMap.get(m.getId())){//在匝道附近或者认为已上车
//                    //不是第一次有数据，已经初始化nowmap，遍历nowmap，看是否有车没了,也就是nowmap有，目前没有
//                    for (Map.Entry<String, javafx.util.Pair<Boolean,Integer>> entry : nowMap.entrySet()) {
//                        String key = entry.getKey();
//                        if(tempMap.get(key)==null){//如果当前这辆车目前没有但是nowmap有，视为可能缺失
//                            if(nowMap.get(key).getKey()){//==true就是还是三次以下 ==false就是已经消失三次以上，视为没了
//                                tempMap.put(key, new javafx.util.Pair<>(true, nowMap.get(key).getValue() + 1));
//                                if(tempMap.get(key).getValue()>3){
//                                    tempMap.put(key, new javafx.util.Pair<>(false, nowMap.get(key).getValue()));
//                                    if(PointMap.get(key).getMileage()>=1121970&&PointMap.get(key).getMileage()<=1121990&&PointMap.get(key).getDirection()==1){
//                                        zaMap.put(key,"CK");
//                                    }
//                                    else if(PointMap.get(key).getMileage()>=1122544&&PointMap.get(key).getMileage()<=1122564&&PointMap.get(key).getDirection()==2){
//                                        zaMap.put(key,"AK");
//                                    }
//                                }
//                            }else tempMap.put(key, new javafx.util.Pair<>(false, nowMap.get(key).getValue() + 1));
//                        }
//                    }
//                    tempMap.forEach((key, value) -> {
//                        if(!value.getKey()){//不在路上
//                            if(zaMap.get(key)!=null){//上了匝道
//                                try {
//                                    collector.collect(zhadaoPredictNextOne(PointMap.get(key)));
//                                } catch (IOException e) {
//                                    throw new RuntimeException(e);
//                                }
//                            }else{//没上匝道但数据断连
//                                try {
//                                    collector.collect(PredictNextOne(PointMap.get(key)));
//                                } catch (IOException e) {
//                                    throw new RuntimeException(e);
//                                }
//                            }
//                        }else{
//                            collector.collect(PointMap.get(key));
////                            myTools.printPathPointData(PointMap.get(key));
//                        }
//                    });
////                        }
//                    //更新nowmap
//                    nowMap=tempMap;
//
//                }
//            }
//        });
//        // 为合并后的光栅数据流添加键控
//        KeyedStream<PathTData, Long> keyedTrajeStream = mergedStream.keyBy(traje -> 1L);
//
//        KeyedStream<JSONObject, Long> keyedGantryStream = gantryStringSource.transform("ParseJSON",
//                TypeInformation.of(JSONObject.class),
//                new ProcessOperator<>(new ProcessFunction<String, JSONObject>() {
//                    @Override
//                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
//                        JSONObject json = JSON.parseObject(value.replaceAll("^\\[", "").replaceAll("\\]$", ""));
//                        JSONObject params = json.getJSONObject("params");
//                        params.remove("plateImage");
//                        params.remove("headImage");
//                        json.put("params", params);
//                        if(json.getString("deviceId").equals("2714AC28-984A-42E9-A001-36395F243E99")) {
//                            json.put("direction", 2);
//                            json.put("mileage", 16960);
//                        }
//                        else if(json.getString("deviceId").equals("A840C67A-6F9D-4EB8-A00C-44F73E540901")) {
//                            json.put("direction", 1);
//                            json.put("mileage", 14085);
//                        }
//                        out.collect(json);
//                    }
//                })
//        ).keyBy(gantry -> 1L);
//
//        keyedTrajeStream.connect(keyedGantryStream)
//                .process(new TrajectoryEnricher())
//                .print();
//
//
//
//        env.execute("ZMQ Flink Job");
//    }
//    private static PathPoint PredictNextOne(PathPointData pathPointData) throws IOException {
//        long currentTime = System.currentTimeMillis();
//        Float predictedSpeed = calculateMovingAverage(pathPointData.getSpeedWindow());
//        pathPointData.getSpeedWindow().addLast(predictedSpeed);
//        pathPointData.getSpeedWindow().removeFirst();
//        //计算以预测速度跑了多远
//        double distanceDiff = myTools.calculateDistance(predictedSpeed,250); // 米
//        double newTpointno=0;
//        if(pathPointData.getDirection()==1) {
//            newTpointno = pathPointData.getMileage() + distanceDiff; // 更新里程点
//        }else {
//            newTpointno = pathPointData.getMileage() - distanceDiff; // 更新里程点
//        }
//        pathPointData.setMileage((int)newTpointno);
//        String carid = pathPointData.getId();
//        pathPointData.setSpeed(predictedSpeed);
//        pathPointData.setTimestamp(myTools.toDateTimeString(currentTime));
//        PathPoint p=new PathPoint(pathPointData.getDirection(),carid,pathPointData.getLaneNo(),(int)newTpointno,pathPointData.getPlateNumber(),predictedSpeed, pathPointData.getTimeStamp());
////        myTools.printPathPoint(p);
//        return p;
//        }
//
//    private static PathPoint zhadaoPredictNextOne(PathPointData data) throws IOException {
//        long currentTime = System.currentTimeMillis();
//        PathPoint p = null;
//        //经过的时间少于某一值（设置时间限制，避免一直计算预测）
//            if (currentTime-data.getLastReceivedTime()<80000000) {
//                // 使用车辆独立窗口计算
//                Float predictedSpeed = calculateMovingAverage(data.getSpeedWindow());
//                data.getSpeedWindow().addLast(predictedSpeed);
//                data.getSpeedWindow().removeFirst();
//                double distanceDiff = myTools.calculateDistance(predictedSpeed,250); // 米
//                double newTpointno=0;
//                if(data.getDirection()==1) {
//                    newTpointno = data.getMileage() + distanceDiff; // 更新里程点
//                }else {
//                    newTpointno = data.getMileage() - distanceDiff; // 更新里程点
//                }
//                data.setMileage((int)newTpointno);
//                String carid = data.getId();
//                data.setSpeed(predictedSpeed);
//                data.setTimestamp(myTools.toDateTimeString(currentTime));
//                // 输出预测结果
//                Location location1 = LocationOP.getLocation("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\"+zaMap.get(carid)+"_locations.json", newTpointno);
//                Location location2 = LocationOP.getLocation("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\"+zaMap.get(carid)+"_locations.json", newTpointno + (int) (calculateMovingAverage(data.getSpeedWindow()) * 0.2));
//                p=new PathPoint(data.getDirection(),carid,data.getLaneNo(),(int)newTpointno,data.getPlateNumber(),predictedSpeed, data.getTimeStamp());
////                myTools.printPathPoint(p);
//        }
//        return p;
//    }
//    private static float calculateMovingAverage(LinkedList<Float> speedWindow) {
//        synchronized (speedWindow) {
//            return (float) speedWindow.stream()
//                    .mapToDouble(Float::doubleValue)
//                    .average()
//                    .orElse(Double.NaN);
//        }
//    }
//    private static void updatePathTData(PathTData pathTData) {
//        for (PathPoint Point : pathTData.getPathList()) {//遍历当前这条TrajeData消息中的所有TrajePoint
//            PathPointData data = PointMap.compute(Point.getId(), (k,v) -> new PathPointData());
//            synchronized (data) {
//                data.getSpeedWindow().add(Point.getSpeed());
//                if (data.getSpeedWindow().size() > WINDOW_SIZE) {
//                    data.getSpeedWindow().removeFirst();
//                }
//                data.setMileage(Point.getMileage());
//                data.setId(Point.getId());
//                data.setDirection(Point.getDirection());
//                data.setLaneNo(Point.getLaneNo());
//                data.setMileage(Point.getMileage());
//                data.setPlateNumber(Point.getPlateNumber());
//                data.setTimestamp(Point.getTimeStamp());
//                data.setLastReceivedTime(pathTData.getTime());
//                data.setSpeed(Point.getSpeed());
//            }
//        }
//    }
//    private static void FirstEnterputNowMap(PathTData pathTData) throws IOException {
//        List<PathPoint> p=pathTData.getPathList();
//        for(PathPoint m:p){
//            nowMap.put(m.getId(),new javafx.util.Pair<>(true,0));
//        }
//    }
//    // 只是在不实际分区的情况下适用，因为到处都是调用函数，所以状态一定全进程共享
//    public static class TrajectoryEnricher extends CoProcessFunction<PathTData, JSONObject, PathTData> {
//
//        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(5))
//                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                .build();
//
//        // 这其实可以设置定期检测上一次匹配到门架的时间，超时则删除整个
//        // 真实作业环境中，目前考虑用Redis组为全局缓存层，当检测到某个CarId的车辆的轨迹输出时，则相应更新vehicleState
//        StateTtlConfig vehiclettlConfig = StateTtlConfig
//                .newBuilder(Time.seconds(80)) // 设置状态存活时间为 80 秒，此时间为保底时间
//                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // 每次写入时更新存活时间
//                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 不返回过期数据
//                .build();
//
//        // 使用 MapState 缓存门架数据：Key 是门架数据的matchTime，Value 是门架数据
//        private transient MapState<Long, List<JSONObject>> gantryState;
//        // 使用 MapState 缓存车牌匹配数据：Key 是门架数据的carID，Value 是匹配情况记录
//        // 这个需要手动更新，在真实应用场景下，是要在redis缓存中全局更新，更新逻辑应该和“车辆轨迹表”相关联，当车辆轨迹表的轨迹输出时，代表该车已经驶离高速
//        private transient MapState<String, VehicleMapping> vehicleState;
//        // 记录达成阈值的carId和车牌号，注意这里是单线程所以可以用HashMap，而且这里不可能记录到
//        private transient Map<String, String> fineMatchState = new HashMap<>();
//        // 无牌车的阈值要根据项目实际
//        private final int NONE_PLATE_THRESHOLD = 5;
//        // 有牌车的阈值要根据项目实际
//        private final int PLATE_THRESHOLD = 10;
//
//
//        @Override
//        public void open(Configuration parameters) {
//            // 定义门架数据缓存状态（Key 是门架时间戳）
//            MapStateDescriptor<Long, List<JSONObject>> gantryDescriptor =
//                    new MapStateDescriptor<>("gantryState", Types.LONG, TypeInformation.of(new TypeHint<List<JSONObject>>() {
//                    }));
//            gantryDescriptor.enableTimeToLive(ttlConfig);
//            gantryState = getRuntimeContext().getMapState(gantryDescriptor);
//
//            MapStateDescriptor<String, VehicleMapping> vehicleDescriptor =
//                    new MapStateDescriptor<>("vehicleState", Types.STRING, TypeInformation.of(VehicleMapping.class));
//            vehicleDescriptor.enableTimeToLive(vehiclettlConfig);
//            vehicleState = getRuntimeContext().getMapState(vehicleDescriptor);
//        }
//
//        // 接收光栅数据，执行实时匹配
//        @Override
//        public void processElement1(PathTData traje, Context ctx, Collector<PathTData> out) throws Exception {
//            updateVehicleState(traje);
//
//            matchGantry(traje);
//
//            for (PathPoint ppoint : traje.getPathList()) {
//                if (fineMatchState.containsKey(ppoint.getId()))
//                    ppoint.setPlateNumber(fineMatchState.get(ppoint.getId()));
//                else if (Objects.equals(ppoint.getPlateNumber(), ""))
//                    ppoint.setPlateNumber(vehicleState.get(ppoint.getId()).getLastMMPlate());
//            }
//
//            out.collect(traje);
//        }
//
//        // 处理并缓存门架数据
//        @Override
//        public void processElement2(JSONObject gantry, Context ctx, Collector<PathTData> out) throws Exception {
//            long recvGantryTs = System.currentTimeMillis();
//            // 初步筛选门架数据
//            if (!(gantry.get("deviceId")).equals("2714AC28-984A-42E9-A001-36395F243E99"))
//                return;
//            else if (!gantry.getJSONObject("params").getString("plateNumber").equals("默A00000") ||
//                    !gantry.getJSONObject("params").getJSONObject("tollRecord").getJSONArray("tollList").isEmpty()) {
//                String plateNumber = gantry.getJSONObject("params").getString("plateNumber");
//                if(plateNumber.equals("默A00000"))
//                    plateNumber = gantry.getJSONObject("params").getJSONObject("tollRecord").getJSONArray("tollList").getJSONObject(0).getString("tollPlateNumber");
//                if(fineMatchState.containsKey(plateNumber))
//                    return;
//            }
//            else if(gantry.getJSONObject("params").getString("envState").equals("99"))
//                return;
//
//            long gantryTimestamp = convertToTimestamp(gantry.getJSONObject("params").getString("uploadTime"));
//
//            long matchTime;
//            if (recvGantryTs / 1000 == gantryTimestamp / 1000)
//                matchTime = (recvGantryTs / 1000) * 1000 + ((recvGantryTs % 1000) / 200) * 200 - 50;
//            else if (recvGantryTs / 1000 > gantryTimestamp / 1000)
//                matchTime = (recvGantryTs / 1000) * 1000 - 50;
//            else
//                matchTime = gantryTimestamp - 50;
//
//            List<JSONObject> gantryList;
//            if (!gantryState.contains(matchTime)) {
//                gantryList = new ArrayList<>();
////                System.out.println("新的gantryList被创建");
//            } else
//                gantryList = gantryState.get(matchTime);
//
//            // 将数据添加到桶中
//            gantryList.add(gantry);
//            gantryState.put(matchTime, gantryList);
//
//        }
//
//        private void updateVehicleState(PathTData traje) throws Exception {
//            for (PathPoint ppoint : traje.getPathList()) {
//                String id = ppoint.getId();
//                if (vehicleState.contains(id))
//                    continue;
//                else
//                    vehicleState.put(id, new VehicleMapping());
//            }
//        }
//
//        private List<JSONObject> strictPlateMacth(PathTData traje, List<JSONObject> gantryList) throws Exception {
//            Iterator<JSONObject> iterator = gantryList.iterator();
//            while (iterator.hasNext()) {
//                JSONObject gantry = iterator.next();
//                // 首先过滤天气不好的情况
//                // gantry.getJSONObject("params").getString("palteNumber") == "默A00000" && gantry.getJSONObject("params").getInteger(headLaneCode) == null
//                if (!gantry.getJSONObject("params").getString("envState").equals("99")) {
//                    for (PathPoint ppoint : traje.getPathList()) {
//                        if (!Objects.equals(ppoint.getPlateNumber(), "") ||
//                                ppoint.getDirection() != gantry.getInteger("direction") ||
//                                ppoint.getMileage() < gantry.getInteger("mileage") ||
//                                ppoint.getLaneNo() != (gantry.getJSONObject("params")).getInteger("headLaneCode"))
//                            continue;
//                        else {
//                            setMacthedPlate(gantry, ppoint);
//                            System.out.println("匹配到了gantry：" + gantry);
//                            // 删除匹配到的门架数据
//                            iterator.remove();
//                        }
//                    }
//                }
//            }
//            return gantryList;
//        }
//
//        private List<JSONObject> relaxedPlateMatch(PathTData traje, List<JSONObject> gantryList) throws Exception {
//            Iterator<JSONObject> iterator = gantryList.iterator();
//            List<PathPoint> suitPoints = new ArrayList<>();
//            while (iterator.hasNext()) {
//                JSONObject gantry = iterator.next();
//                if (!gantry.getJSONObject("params").getString("envState").equals("99")) {
//                    for (PathPoint ppoint : traje.getPathList()) {
//                        // 严格限制：已经匹配过的和方向不同的不参与匹配
//                        // 直接在gantry里加上方向！！
//                        if (!Objects.equals(ppoint.getPlateNumber(), "") ||
//                                ppoint.getDirection() != gantry.getInteger("direction"))
//                            continue;
//                        // 放宽条件1：车道允许相邻（如压线行驶）
//                        // 发现在刚进合流车道到就会被拍到的情况，所以有特殊情况，laneNo == 5，但是因为设置了延迟匹配，所以目前还是不加
//                        boolean laneTolerance = Math.abs(ppoint.getLaneNo() - gantry.getJSONObject("params").getInteger("headLaneCode")) <= 1;
////                        boolean laneTolerance = (Math.abs(ppoint.getLaneNo() - gantry.getJSONObject("params").getInteger("headLaneCode")) <= 1 ||
////                                                    ppoint.getLaneNo() == 5);
//
//                        // 放宽条件2：里程允许车辆超过门架最多50米
//                        boolean mileageTolerance = ppoint.getMileage() >= (gantry.getInteger("mileage") - 50);
//
//                        // 这里在正式匹配车牌之前应该先确定具体匹配哪一个点，有可能此时刻车辆很多
//                        if (laneTolerance && mileageTolerance)
//                            suitPoints.add(ppoint);
//                    }
//                    if(suitPoints.size() == 1) {
//                        PathPoint ppoint = suitPoints.get(0);
//                        setMacthedPlate(gantry, ppoint);
//                        System.out.println("第二次匹配，匹配到了gantry：" + gantry);
//                        // 删除匹配到的门架数据
//                        iterator.remove();
//                    }
//                    else if(suitPoints.size() > 1) {
//                        // 找到最合适的匹配点
//                        // 目前感觉延迟出现的概率可能高一点
//                        PathPoint ppoint;
//                        ppoint = suitPoints.stream()
//                                .filter(pathPoint -> pathPoint.getLaneNo() == gantry.getJSONObject("params").getInteger("headLaneCode"))
//                                .min(Comparator.comparingInt(pathPoint -> Math.abs(pathPoint.getMileage() - gantry.getJSONObject("params").getInteger("mileage"))))
//                                .orElse(null);
//
//                        if(ppoint != null) {
//                            setMacthedPlate(gantry, ppoint);
//                            System.out.println("第二次匹配，匹配到了gantry：" + gantry);
//                            // 删除匹配到的门架数据
//                            iterator.remove();
//                        }
//                        else {
//                            System.out.println("应该是出现了压线情况，下面进行纯扩距离匹配");
//                            // 此时一定会有一个匹配结果
//                            ppoint = suitPoints.stream()
//                                    .min(Comparator.comparingInt(pathPoint -> Math.abs(pathPoint.getMileage() - gantry.getJSONObject("params").getInteger("mileage"))))
//                                    .orElse(null);
//                            setMacthedPlate(gantry, ppoint);
//                            System.out.println("第二次匹配，匹配到了gantry：" + gantry);
//                            // 删除匹配到的门架数据
//                            iterator.remove();
//                        }
//                    }
//                }
//            }
//            return gantryList;
//        }
//
//        private List<JSONObject> lastPlateMacth(PathTData traje, List<JSONObject> gantryList) throws Exception {
//            Iterator<JSONObject> iterator = gantryList.iterator();
//            while (iterator.hasNext()) {
//                JSONObject gantry = iterator.next();
//                // 仅限天气不好
//                if (gantry.getJSONObject("params").getString("envState").equals("99")) {
//                    for(PathPoint ppoint : traje.getPathList()){
//                        // 严格限制：已经匹配过的和方向不同的不参与匹配
//                        if (!Objects.equals(ppoint.getPlateNumber(), "") ||
//                                ppoint.getDirection() != gantry.getInteger("direction"))
//                            continue;
//                        // 宽松的距离匹配
//                        if(ppoint.getMileage() >= (gantry.getInteger("mileage") - 50)){
////                            String mactchedPlate = gantry.getJSONObject("params").getJSONArray("tollList").getJSONObject(0).getString("tollPlateNumber");
////                            Map<String, Integer> plateCounts = vehicleState.get(ppoint.getId()).getPlateCounts();
////                            if (plateCounts.containsKey(mactchedPlate))
////                                plateCounts.put(mactchedPlate, plateCounts.get(mactchedPlate) + 1);
////                            else
////                                plateCounts.put(mactchedPlate, 1);
////                            ppoint.setPlateNumber(vehicleState.get(ppoint.getId()).getMostMatchedPlate());
//                            setMacthedPlate(gantry, ppoint);
//                            System.out.println("最终匹配，匹配到了gantry：" + gantry);
//                            // 删除匹配到的门架数据
//                            iterator.remove();
//                        }
//                    }
//                }
//            }
//            return gantryList;
//        }
//
//        private void setMacthedPlate(JSONObject gantry, PathPoint ppoint) throws Exception {
//            String mactchedPlate;
//            if(!gantry.getJSONObject("params").getString("plateNumber").equals("默A00000"))
//                mactchedPlate = gantry.getJSONObject("params").getString("plateNumber");
//            else if (!gantry.getJSONObject("params").getJSONObject("tollRecord").getJSONArray("tollList").isEmpty())
//                mactchedPlate = gantry.getJSONObject("params").getJSONObject("tollRecord").getJSONArray("tollList").getJSONObject(0).getString("tollPlateNumber");
//            else
//                mactchedPlate = "默A00000";
//            if(mactchedPlate.equals("默A00000")) {
//                if(vehicleState.get(ppoint.getId()).getLastMMPlate().equals("") ||
//                        vehicleState.get(ppoint.getId()).getLastMMPlate().equals(("默A00000"))) {
//                    ppoint.setPlateNumber("默A00000");
//                    int defaultPlateSum = vehicleState.get(ppoint.getId()).getDefaultPlateSum();
//                    vehicleState.get(ppoint.getId()).setDefaultPlateSum(defaultPlateSum + 1);
//                    if(defaultPlateSum + 1 >= NONE_PLATE_THRESHOLD) {
//                        vehicleState.remove(ppoint.getId());
//                        fineMatchState.put(ppoint.getId(), "无牌车");
//                        return;
//                    }
//                }
//                else {
//                    // 极端特殊情况，ETC中途未识别到
//                    ppoint.setPlateNumber(vehicleState.get(ppoint.getId()).getLastMMPlate());
//                }
//            }
//            else {
//                Map<String, Integer> plateCounts = vehicleState.get(ppoint.getId()).getPlateCounts();
//                if (plateCounts.containsKey(mactchedPlate))
//                    plateCounts.put(mactchedPlate, plateCounts.get(mactchedPlate) + 1);
//                else
//                    plateCounts.put(mactchedPlate, 1);
//                Pair<String, Integer> result = vehicleState.get(ppoint.getId()).getMostMatchedPlate();
//                String mostMacthedPlate = result.getLeft();
//                ppoint.setPlateNumber(mostMacthedPlate);
//                vehicleState.get(ppoint.getId()).setLastMMPlate(mostMacthedPlate);
//                if(result.getRight() >= PLATE_THRESHOLD) {
//                    fineMatchState.put(mactchedPlate, ppoint.getId());
//                    vehicleState.remove(ppoint.getId());
//                    return;
//                }
//            }
//            vehicleState.get(ppoint.getId()).setLastUpdateTime(convertToTimestampMillis(ppoint.getTimeStamp()));
//        }
//
//        public void matchGantry(PathTData traje) throws Exception {
//            String outputFile = "D:\\temp\\MatchedLeftGantries.txt";  // 输出文件名
//
//            try(FileWriter fileWriter = new FileWriter(outputFile, true);
//                PrintWriter printWriter = new PrintWriter(fileWriter)) {
//
//                long trajeTs = traje.getTime();
//                if (!gantryState.isEmpty()) {
//                    if (gantryState.contains(trajeTs)) {
//                        List<JSONObject> gantryList = new ArrayList<>(gantryState.get(trajeTs));
//                        // 执行初次匹配
//                        List<JSONObject> macth1Remain = new ArrayList<>(strictPlateMacth(traje, gantryList));
//                        gantryState.put(trajeTs, macth1Remain);
//                        if (!macth1Remain.isEmpty()) {
//                            // 二次匹配
//                            List<JSONObject> macth2Remain = new ArrayList<>(relaxedPlateMatch(traje, macth1Remain));
//                            gantryState.put(trajeTs, macth2Remain);
//                            if (!macth2Remain.isEmpty()) {
//                                // 极端匹配
//                                List<JSONObject> macth3Remain = new ArrayList<>(lastPlateMacth(traje, macth2Remain));
//                                gantryState.put(trajeTs, macth3Remain);
//                                if (!macth3Remain.isEmpty()) {
//                                    for (JSONObject gantry : macth3Remain) {
//                                        // 针对个别延迟：至多延长至下一次再匹配一次
//                                        if (gantry.getJSONObject("params").getString("plateNumber") != "默A00000" &&
//                                                gantry.getJSONObject("params").getInteger("headLaneCode") != null &&
//                                                trajeTs + 200 <= convertToTimestamp(gantry.getJSONObject("params").getString("uploadTime")) + 1150) {
//                                            if (gantryState.contains(trajeTs + 200)) {
//                                                List<JSONObject> nextGantryList = new ArrayList<>(gantryState.get(trajeTs + 200));
//                                                nextGantryList.add(gantry);
//                                                gantryState.put(trajeTs + 200, nextGantryList);
//                                            } else
//                                                gantryState.put(trajeTs + 200, new ArrayList<>(Arrays.asList(gantry)));
//                                            continue;
//                                        }
//                                        printWriter.println(gantry);
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//                // 前提是光栅数据相比于门架数据有天然的延迟，目前基本都大于1400ms
//                gantryState.remove(trajeTs);
//            }
//        }
//    }
//}