//package whu.edu.ljj.flink.merge.Version1;
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
//import org.apache.flink.api.common.typeinfo.TypeHint;
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
//import org.apache.flink.util.OutputTag;
//
//import java.io.IOException;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
//import static whu.edu.ljj.flink.xiaohanying.Utils.*;
//
//import whu.edu.ljj.flink.utils.JsonReader;
//import whu.edu.ljj.flink.utils.LocationOP;
//import whu.edu.ljj.flink.utils.myTools;
//import whu.edu.ljj.flink.xiaohanying.ZmqSource;
//
//
//
///**
// * Special Edition new-Local v7 for 孝汉应
// * 用于融合模块测试
// * 暂时去除写入kafka部分
// */
//public class TGCTime1stLocalV7 {
//    // 京港澳公路有50个卡口，记得写静态代码块初始化GantryAssignment
//    private static GantryAssignment gantryAssign;
//
//    // 根据光栅数据事件格式确定参与计算的修正数
//    private static long d1TimeInterval;
//    private static boolean isD1TimeInitialized = false;
//    private static long d2TimeInterval;
//    private static boolean isD2TimeInitialized = false;
//
//    // 定义 OutputTag，使用具体的类类型
////    private static final OutputTag<GantryRecord> GANTRY_RECORD_TAG = new OutputTag<GantryRecord>("allGantries") {};
////    private static final OutputTag<GantryImage> IMAGE_TAG = new OutputTag<GantryImage>("gantryHeadImage"){};
//
//
//    private static final int WINDOW_SIZE = 20;//用来预测的窗口大小
//    private static final Map<Long, PathPointData> PointMap = new ConcurrentHashMap<>();
//    private static final Map<Long, PathPointData> JizhanPointMap = new ConcurrentHashMap<>();//雷视数据获取到的匝道上的所有车
//    static boolean firstEnter=true;
//    static Map<Long, javafx.util.Pair<Boolean,Integer>> nowMap= new ConcurrentHashMap<>();
//    //    carid  是否在路上  数据缺失了几次
//    static Map<javafx.util.Pair<Long,String>,String> zaMap= new ConcurrentHashMap<>();
//    //       carid  carNumber  匝道编号
//    private static final long mainRoadMinMillage=0;//主路上的最小里程
//    private static final long mainRoadMaxMillage=1111111111;//主路上的最大里程
//    static List<Location> roadKDataList;
//    static List<Location> roadAKDataList;
//    static List<Location> roadBKDataList;
//    static List<Location> roadCKDataList;
//    static List<Location> roadDKDataList;
//    private static String pathTimeStamp="";
//    private static float predictedSpeed=0;//预测速度
//    private static double distanceDiff=0;
//    private static long pathTime=0;
//    private static int tcount=0;
//    private static long t1=0;
//    private static boolean tb1=true;
//    private static boolean tb2=true;
//    private static long t2=0;
//    private static int newscount=0;
//    static {
//        try {
//            roadKDataList  = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\K_locations.json");
//            roadAKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\AK_locations.json");
//            roadBKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\BK_locations.json");
//            roadCKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\CK_locations.json");
//            roadDKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\DK_locations.json");
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        try {
//            gantryAssign = new GantryAssignment("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\merge\\Version1\\孝汉应卡口.xlsx");
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
////        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000); // 增加超时时间
////        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
//        env.setParallelism(4);
//
//        // 京港澳公路有多段光栅，记得写自动创建ZMQ数据源的代码
//
//        // 创建 ZMQ 数据源
//        KeyedStream<TrajeData, Long> trajeStreamD1 = env.addSource(new ZmqSource("tcp://100.65.62.82:8030"))
//                .map(trajejson -> {
//                    TrajeData trajeData = JSON.parseObject(String.valueOf(trajejson), TrajeData.class);
////                    if(!isD1TimeInitialized) {
////                        d1TimeInterval = (200 - (trajeData.getTIME() % 1000 % 200)) % 200;
////                        System.out.println("d1TimeInterval："+d1TimeInterval);
////                        isD1TimeInitialized = true;
////                    }
////                    else
////                    {
////                        if (d1TimeInterval != (200 - (trajeData.getTIME() % 1000 % 200)) % 200)
////                            d1TimeInterval = 200 - (trajeData.getTIME() % 1000 % 200) % 200;
////
////                    }
//                    // 每次都计算一下
//                    d1TimeInterval = (200 - (trajeData.getTIME() % 1000 % 200)) % 200;
//                    isD1TimeInitialized = true;
//                    return trajeData;
//                })
//                .keyBy(trajeData -> trajeData.getTIME());
//
//        KeyedStream<TrajeData, Long> trajeStreamD2 = env.addSource(new ZmqSource("tcp://100.65.62.82:8040"))
//                .map(trajejson -> {
//                    TrajeData trajeData = JSON.parseObject(String.valueOf(trajejson), TrajeData.class);
//                    if(!isD2TimeInitialized) {
//                        d2TimeInterval = (200 - (trajeData.getTIME() % 1000 % 200)) % 200;
////                        System.out.println("d2TimeInterval："+d2TimeInterval);
//                        isD2TimeInitialized = true;
//                    }
//                    if(isD1TimeInitialized && d1TimeInterval != d2TimeInterval) {
//                        long fixTime = d2TimeInterval - d1TimeInterval;
//                        if(fixTime > 0)
//                            trajeData.setTIME(trajeData.getTIME() - (200 - fixTime));
//                        else
//                            trajeData.setTIME(trajeData.getTIME() + (200 + fixTime));
//                    }
//                    // 这里其实就表明，如果d1还没初始化，先到的d2数据是不参与后续计算的
//                    return trajeData;
//                })
//                .keyBy(trajeData -> trajeData.getTIME());
//
//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setTopics("rhy_iot_receive_lpr_bizAttr")
//                .setBootstrapServers("100.65.62.6:9092")
//                .setGroupId("gantry-group")
//                .setStartingOffsets(OffsetsInitializer.latest())
//                .setProperty("auto.offset.commit", "true")
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//
//        DataStreamSource<String> gantryStringSource = env.fromSource(kafkaSource,
//                WatermarkStrategy.noWatermarks(),
//                "Gantry Source");
//
//        SingleOutputStreamOperator<GantryData> gantryDataStream = gantryStringSource.transform("ParseJSON",
//                TypeInformation.of(GantryData.class),
//                new ProcessOperator<>(new ProcessFunction<String, GantryData>() {
//                    @Override
//                    public void processElement(String value, ProcessFunction<String, GantryData>.Context ctx, Collector<GantryData> out) throws Exception {
//                        JSONObject gantry = JSON.parseObject(value.replaceAll("^\\[", "").replaceAll("\\]$", ""));
//
//                        gantry.getJSONObject("params").remove("plateImage");
//                        gantry.getJSONObject("params").remove("headImage");
//
//                        // 初步筛选门架数据
//                        if (!gantry.getString("deviceId").equals("2714AC28-984A-42E9-A001-36395F243E99") &&
//                                !gantry.getString("deviceId").equals("A840C67A-6F9D-4EB8-A00C-44F73E540901"))
//                            return;
//                        if(gantry.getJSONObject("params").getString("envState").equals("99") &&
//                                gantry.getJSONObject("params").getJSONObject("tollRecord").getJSONArray("tollList").isEmpty())
//                            return;
//
//                        // gantryData
//                        GantryData gData = new GantryData();
//                        gData.setId(gantry.getString("deviceId"));
//                        gData.setDirection(gantryAssign.getGantriesByID().get(gantry.getString("deviceId")).getDirection());
//                        gData.setMileage(gantryAssign.getGantriesByID().get(gantry.getString("deviceId")).getMileage());
//                        gData.setUploadTime(gantry.getJSONObject("params").getString("uploadTime"));
//                        gData.setEnvState(gantry.getJSONObject("params").getString("envState"));
//                        gData.setPlateNumber(gantry.getJSONObject("params").getString("plateNumber"));
//                        gData.setPlateColor(Integer.parseInt(gantry.getJSONObject("params").getString("plateColor")));
//                        if(!gantry.getJSONObject("params").getJSONObject("tollRecord").getJSONArray("tollList").isEmpty())
//                        {
//                            JSONObject tollRecord1 = gantry.getJSONObject("params").getJSONObject("tollRecord").getJSONArray("tollList").getJSONObject(0);
//                            // gantryData
//                            gData.setTollPlateNumber(tollRecord1.getString("tollPlateNumber"));
//                            gData.setTollPlateColor(tollRecord1.getInteger("tollPlateColor"));
//                            gData.setTollFeeVehicleType(tollRecord1.getInteger("tollFeeVehicleType"));
//                            // 暂时保存tollVehicleUserType
//                            gData.setTollVehicleUserType(tollRecord1.getInteger("tollVehicleUserType"));
//                        }
//                        // gantryData
//                        if(!gData.getEnvState().equals("99"))
//                            gData.setHeadLaneCode(gantry.getJSONObject("params").getInteger("headLaneCode"));
//
////                        System.out.println("\n即将要参与匹配的gantry："+gData);
//                        out.collect(gData);
//                    }
//                })
//        );
//        KeyedStream<GantryData, Long> keyedGantryStream = gantryDataStream.keyBy(gantry -> 1L);
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
//                                    ppoint.setDirection((int) point.getDirect());
//                                    ppoint.setId(point.getID());
//                                    ppoint.setLaneNo((int) point.getWayno());
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
//        SingleOutputStreamOperator<PathTData> bufferedMergedStream = mergedStream.keyBy(pathData -> 3L).process(new ProcessFunction<PathTData, PathTData>() {
//            private ListState<PathTData> bufferState;
//            private final StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(5))
//                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                    .build();
//
//            @Override
//            public void open(Configuration parameters) {
//                System.out.println("已经进来了");
//                ListStateDescriptor<PathTData> bufferDescriptor =
//                        new ListStateDescriptor<>("bufferState", TypeInformation.of(new TypeHint<PathTData>() {
//                        }));
//                bufferDescriptor.enableTimeToLive(ttlConfig);
//                bufferState = getRuntimeContext().getListState(bufferDescriptor);
//            }
//
//            @Override
//            public void processElement(PathTData value, ProcessFunction<PathTData, PathTData>.Context ctx, Collector<PathTData> out) throws Exception {
////                System.out.println("此时传进来的PathTData的时间为：" + value.getTime());
//                List<PathTData> bufferData = new ArrayList<>();
//                for (PathTData data : bufferState.get())
//                    bufferData.add(data);
//                if (!bufferData.isEmpty()) {
//                    bufferData.sort(Comparator.comparingLong(PathTData::getTime));
//                    if (bufferData.get(bufferData.size() - 1).getTime() - bufferData.get(0).getTime() >= 3000) {
//                        out.collect(bufferData.get(0));
//                        bufferData.remove(0);
//                    }
//                }
//                bufferData.add(value);
//                bufferState.update(bufferData);
//            }
//        });
//
//        // 为合并后的光栅数据流添加键控
//        KeyedStream<PathTData, Long> keyedTrajeStream = bufferedMergedStream.keyBy(traje -> 1L);
//
//        SingleOutputStreamOperator<PathTData> pathTDataStream = keyedTrajeStream.connect(keyedGantryStream)
//                .process(new TrajectoryEnricherLocalV7(gantryAssign));
//
//        SingleOutputStreamOperator<PathTData> endPathTDataStream=pathTDataStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {
//            @Override//5.56   33.76  86.64
//            public void flatMap(PathTData PathTData, Collector<PathTData> collector) throws Exception {
////                    System.out.println(myTools.toDateTimeString(System.currentTimeMillis()));
//
//                t1 = System.currentTimeMillis();
//
//                pathTimeStamp=PathTData.getTimeStamp();
//                pathTime= PathTData.getTime();
//                PathTData pathTData = new PathTData();
//                pathTData.setTime(pathTime);
//                pathTData.setTimeStamp(pathTimeStamp);
//                pathTData.setPathNum(PathTData.getPathNum());
//                pathTData.setWaySectionId(PathTData.getWaySectionId());
//                pathTData.setWaySectionName(PathTData.getWaySectionName());
//                List<PathPoint> list=new ArrayList<>();
//                //如果mergedata合法
//                if (!PathTData.getPathList().isEmpty()) {
//                    //存车辆id对应的车辆是否在车道上、几次没有出现
//                    Map<Long,Pair<Boolean,Integer>> tempMap= new ConcurrentHashMap<>();
//
////                        b=true;
//                    updateMergePoint(PathTData);//更新当前车辆map
//                    List<PathPoint> p=PathTData.getPathList();
//                    for (PathPoint m : p) {
////                                try (BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\mergedata\\result\\data2\\"+m.getId()+".txt",true))) {
////                                    writer.write("ID:"+m.getId()+" SKID:"+m.getStakeId()+"  timeStamp:"+m.getTimeStamp());
////                                    writer.write(System.lineSeparator());
////                                }
//                        if (firstEnter) {//第一次有数据，初始化nowmap
//                            FirstEnterputNowMap(PathTData);
//                            tempMap = nowMap;
//                            firstEnter = false;
//                        } else {
//                            tempMap.put(m.getId(), new Pair<>(true, 0));
//                        }
//                    }
////                            try (BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\mergedata\\my.txt",true))) {
//                    newscount++;
////                            tempMap.forEach((key, value) -> {
////                                if(myTools.getNString(PointMap.get(key).getStakeId(),0,1).equals("AK")){
////
////                                };
////                            });
//                    //不是第一次有数据，已经初始化nowmap，遍历nowmap，看是否有车没了,也就是nowmap有，目前没有
//                    //里程+数据丢失检测 上匝道
//                    for (Map.Entry<Long, Pair<Boolean,Integer>> entry : nowMap.entrySet()) {
//                        long key = entry.getKey();
//                        Pair<Boolean, Integer> now = nowMap.get(key);
//                        PathPointData pathPointData = PointMap.get(key);
//                        if(pathPointData!=null) {
//                            if (tempMap.get(key) == null && pathPointData.getMileage() != null) {//如果当前这辆车目前没有但是nowmap有，视为可能缺失。
//                                // 上面pathPointData.getMileage()!=null是因为模拟数据有的没mileage
//                                if (now.getKey()) {//==true就是还是三次以下 ==false就是已经消失三次以上，视为没了
//                                    tempMap.put(key, new Pair<>(true, now.getValue() + 1));
//                                    if (tempMap.get(key).getValue() == 1) {
//                                        tempMap.put(key, new Pair<>(false, now.getValue()));
//                                        if (pathPointData.getMileage() >= 1121970 && pathPointData.getMileage() <= 1121990 && pathPointData.getDirection() == 1) {
//                                            zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "CK");
//                                        } else if (pathPointData.getMileage() >= 1122544 && pathPointData.getMileage() <= 1122564 && pathPointData.getDirection() == 2) {
//                                            zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "AK");
//                                        }
////                                        mark:移出zaMap的逻辑
//                                    }
//                                } else tempMap.put(key, new Pair<>(false, now.getValue() + 1));
//                            }
//                        }
//                    }
//                    PointMap.forEach((k,v)->{
//                        if(v.getMileage()==null)PointMap.remove(k);
//                    });
//                    JizhanPointMap.forEach((k,v)->{
//                        if(v.getMileage()==null)JizhanPointMap.remove(k);
//                    });
//                    for (Map.Entry<Long, Pair<Boolean,Integer>> entry : tempMap.entrySet()) {
//                        long key =entry.getKey();
//                        PathPointData pathPointData =PointMap.get(key);
//                        if(pathPointData!=null) {
//                            if (pathPointData.getStakeId() != null) {
//                                String nString = myTools.getNString(pathPointData.getStakeId(), 0, 2);
//                                switch (nString) {
//                                    case "AK":
//                                        zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "AK");
//                                        tempMap.put(key,new Pair<>(false,3));
////                                            System.out.println(zaMap.get(new Pair<>(key, pathPointData.getPlateNo())));
//                                        break;
//                                    case "BK":
//                                        zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "BK");
//                                        tempMap.put(key,new Pair<>(false,3));
//                                        break;
//                                    case "CK":
//                                        zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "CK");
//                                        tempMap.put(key,new Pair<>(false,3));
//                                        break;
//                                    case "DK":
//                                        zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "DK");
//                                        tempMap.put(key,new Pair<>(false,3));
//
//                                        break;
//                                    case "K":
//                                        if(zaMap.get(new Pair<>(key, pathPointData.getPlateNo()))!=null){
//                                            zaMap.remove(new Pair<>(key, pathPointData.getPlateNo()));
//                                            tempMap.put(key,new Pair<>(true,0));
//                                            nowMap.put(key,new Pair<>(true,0));
//                                        }
//                                        break;
//                                }
//                            }
//                        }
//                    }
//                    for (Map.Entry<Long, PathPointData> entry : JizhanPointMap.entrySet()) {
//                        long key =entry.getKey();
//                        PathPointData pathPointData =PointMap.get(key);
//                        if(pathPointData!=null) {
//                            String nString = myTools.getNString(PointMap.get(key).getStakeId(), 0, 2);
//                            switch (nString) {
//                                case "AK":
//                                    zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "AK");
//                                    break;
//                                case "BK":
//                                    zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "BK");
//                                    break;
//                                case "CK":
//                                    zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "CK");
//                                    break;
//                                case "DK":
//                                    zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "DK");
//                                    break;
//                                case "K":
//                                    if(zaMap.get(new Pair<>(key, pathPointData.getPlateNo()))!=null){
//                                        zaMap.remove(new Pair<>(key, pathPointData.getPlateNo()));
//                                        tempMap.put(key,new Pair<>(true,0));
//                                        nowMap.put(key,new Pair<>(true,0));
//                                    }
//                                    break;
//                            }
//                        }
//                    }
//                    tempMap.forEach((key, value) -> {
//                        PathPointData pd= PointMap.get(key);
//                        if(pd!=null) {
//                            //预测的点要加上：mileage、speed、skateID
//                            if (!value.getKey()) {//不在路上
////                                if(pd.getSpeedWindow()==null)System.out.println(pd);
//                                predictedSpeed = calculateMovingAverage(pd.getSpeedWindow());
//                                distanceDiff = myTools.calculateDistance(predictedSpeed, 250); // 米
//                                if (zaMap.get(new Pair<>(key, pd.getPlateNo())) != null) {//id对应的zamap不为null，为上了匝道
////                                    try {
////                                        writer.write("zamap != null:"+zaMap.get(new Pair<>(key, pd.getPlateNo())));
////                                    } catch (IOException e) {
////                                        throw new RuntimeException(e);
////                                    }
//                                    try {
//                                        PathPointData jizhanpoint = JizhanPointMap.get(key);//先看看雷视数据里有没有数据，有就用雷视，没有就预测
//                                        if (jizhanpoint != null) {
//                                            if (System.currentTimeMillis() - jizhanpoint.getLastReceivedTime() < 300) {//相差少于300秒，数据没有缺失，输出雷视数据
//                                                //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
//                                                String sk = jizhanpoint.getStakeId();
//                                                //如果雷视数据的桩号为null，则计算出经纬度，如果不为null,则通过经纬度计算出桩号
//                                                if (sk == null) {
//                                                    //上一个点的location（因为pointmap里存的是上一个点的，现在要预测当前点的）
//                                                    Pair<Location, Integer> kkk = null;
//                                                    try {
//                                                        kkk = LocationOP.UseLLGetSK(jizhanpoint.getLatitude(), jizhanpoint.getLongitude(), roadKDataList);
//                                                    } catch (IOException e) {
//                                                        throw new RuntimeException(e);
//                                                    }
//                                                    jizhanpoint.setStakeId(kkk.getKey().getLocation());
//                                                } else {
//                                                    //question
//                                                    if (jizhanpoint.getLatitude() == 0) {
//                                                        //如果匝道号为"K",则应该在匝道，桩号却在主路上，说明数据有问题
//                                                        if ((myTools.getNString(sk, 0, 1)).equals("K")) return;
//                                                        Location l = LocationOP.UseSKgetLL(sk, roadKDataList, distanceDiff);
//                                                        assert l != null;
//                                                        jizhanpoint.setLatitude(l.getLatitude());
//                                                        jizhanpoint.setLongitude(l.getLongitude());
//                                                    }
//                                                }
//                                                PathPoint pathPoint = PDToPP(jizhanpoint);
//                                                list.add(pathPoint);
//                                            }
//                                        } else {//jizhanpoint == null
//                                            PathPoint pathPoint = zhadaoPredictNextOne(pd);
//                                            updateOneJizhanPointMap(pathPoint);
//                                            updateOnePointMap(pathPoint);
//                                            list.add(pathPoint);
//                                        }
//                                    } catch (IOException e) {
//                                        throw new RuntimeException(e);
//                                    }
//                                } else {//没上匝道但数据断连
//                                    try {
//                                        PathPoint pathPoint = PredictNextOne(pd);
//                                        if (pathPoint != null) {
//                                            list.add(pathPoint);
//                                        }
//
//                                    } catch (IOException e) {
//                                        throw new RuntimeException(e);
//                                    }
//                                }
//                            } else {//if (value.getKey()) {//在路上
//
//                                //因为数据正确，进来的时候就已经改了pointmap，无需再改
//                                //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
//                                String sk = pd.getStakeId();
//                                //如果pd的桩号为null，则计算出经纬度，如果不为null,则通过经纬度计算出桩号
//                                if (sk == null) {
//                                    //上一个点的location（因为pointmap里存的是上一个点的，现在要预测当前点的）
//                                    Pair<Location, Integer> kkk = null;
//                                    try {kkk = LocationOP.UseLLGetSK(pd.getLatitude(), pd.getLongitude(), roadKDataList);} catch (IOException e) {throw new RuntimeException(e);}
//                                    pd.setStakeId(kkk.getKey().getLocation());
//                                } else {//sk !== null
//                                    //question
//                                    if (pd.getLatitude() == 0) {
//                                        //应该在主路，桩号却在匝道上，说明数据有问题
//                                        if (!(myTools.getNString(sk, 0, 1)).equals("K")) return;
//                                        Location l = LocationOP.UseSKgetLL(sk, roadKDataList, distanceDiff);
//                                        assert l != null;
//                                        pd.setLatitude(l.getLatitude());
//                                        pd.setLongitude(l.getLongitude());
//                                    }
//                                }
//                                PathPoint pathPoint = PDToPP(pd);
//                                list.add(pathPoint);
//                                myTools.printPathPoint(pathPoint);
//                            }//if (value.getKey()) {//在路上
//                        }//tempmap的key去取pointmap不为null，最大那个没了
//
//                    });
//                    //整个writter
//                    pathTData.setPathList(list);
//                    collector.collect(pathTData);
//                    //mark:防撞
//                    nowMap=tempMap;
//                }
//
//                t2 = System.currentTimeMillis();
//                System.out.println("time split:"+(t2-t1));
//            }
//
//        });
//
//
////        pathTDataStream.print();
//
//        env.execute("Plate Matching Flink Job");
//    }
//    //匝道上的预测
//    private static PathPoint zhadaoPredictNextOne(PathPointData data) throws IOException {
//        long currentTime = System.currentTimeMillis();
//        PathPoint p = null;
//        //经过的时间少于某一值（设置时间限制，避免一直计算预测）
////    if (currentTime-data.getLastReceivedTime()<80000000) {
//        // 使用车辆独立窗口计算
//        data.getSpeedWindow().addLast(predictedSpeed);
//        data.getSpeedWindow().removeFirst();
//        double newTpointno=0;
//        if(data.getDirection()==1) newTpointno = data.getMileage() + distanceDiff; // 更新里程点
//        else newTpointno = data.getMileage() - distanceDiff; // 更新里程点
//        data.setMileage((int)newTpointno);
//        long carid = data.getId();
//        data.setSpeed(predictedSpeed);
//        data.setTimeStamp(pathTimeStamp);
//        //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
//        String sk=data.getStakeId();
//        Location the =new Location();
//        Location sec = new Location();
//        String whichk=zaMap.get((new Pair<>(carid, data.getPlateNo())));
//        //当前点的location
//        if(sk.isEmpty()) {
//            //下一个点的location及位于第几个
//            Pair<Location, Integer> kkk =null;
//            switch (whichk) {
//                case "AK":
//                    kkk= LocationOP.UseLLGetSK(data.getLatitude(), data.getLongitude(), roadAKDataList);
//                    the = kkk.getKey();
//                    sec = LocationOP.UseDistanceGetThisLocation(distanceDiff, roadAKDataList, kkk.getValue());
//                    break;
//                case "BK":
//                    kkk= LocationOP.UseLLGetSK(data.getLatitude(), data.getLongitude(), roadBKDataList);
//                    the = kkk.getKey();
//                    sec = LocationOP.UseDistanceGetThisLocation(distanceDiff, roadBKDataList, kkk.getValue());
//                    break;
//                case "CK":
//                    kkk= LocationOP.UseLLGetSK(data.getLatitude(), data.getLongitude(), roadCKDataList);
//                    the = kkk.getKey();
//                    sec = LocationOP.UseDistanceGetThisLocation(distanceDiff, roadCKDataList, kkk.getValue());
//                    break;
//                case "DK":
//                    kkk= LocationOP.UseLLGetSK(data.getLatitude(), data.getLongitude(), roadDKDataList);
//                    the = kkk.getKey();
//                    sec = LocationOP.UseDistanceGetThisLocation(distanceDiff, roadDKDataList, kkk.getValue());
//                    break;
//            }
//            data.setStakeId(sec.getLocation());
//        }
//        else{
//            switch (whichk) {
//                case "AK": {
//                    Location l = LocationOP.UseSKgetLL(sk, roadAKDataList,distanceDiff);
//                    assert l != null;
//                    data.setLatitude(l.getLatitude());
//                    data.setLongitude(l.getLongitude());
//                    break;
//                }
//                case "BK": {
//                    Location l = LocationOP.UseSKgetLL(sk, roadBKDataList,distanceDiff);
//                    assert l != null;
//                    data.setLatitude(l.getLatitude());
//                    data.setLongitude(l.getLongitude());
//                    break;
//                }
//                case "CK": {
//                    Location l = LocationOP.UseSKgetLL(sk, roadCKDataList,distanceDiff);
//                    assert l != null;
//                    data.setLatitude(l.getLatitude());
//                    data.setLongitude(l.getLongitude());
//                    break;
//                }
//                case "DK": {
//                    Location l = LocationOP.UseSKgetLL(sk, roadDKDataList,distanceDiff);
//                    assert l != null;
//                    data.setLatitude(l.getLatitude());
//                    data.setLongitude(l.getLongitude());
//                    break;
//                }
//            }
//        }
//        double carangle= myTools.calculateBearing(the.getLatitude(),the.getLongitude(),sec.getLatitude(),sec.getLongitude());
//        //mark:==0还有问题
//        if(data.getCarAngle()==0)data.setCarAngle(carangle);
//        p=new PathPoint(data.getDirection(), data.getId(),data.getLaneNo(),(int) newTpointno, data.getPlateNo(), data.getSpeed(),data.getTimeStamp(), data.getPlateColor(), data.getVehicleType(),data.getLongitude(),data.getLatitude(),data.getCarAngle(),data.getStakeId(),  data.getOriginalType(), data.getPlateColor());
//        myTools.printmergePoint(p);
////    }
//
//        updateOnePointMap(p);
////        myTools.printmergePoint(p);
//        return p;
//    }
//    //主路上的预测
//    private static PathPoint PredictNextOne(PathPointData data) throws IOException {
//        data.getSpeedWindow().addLast(predictedSpeed);
//        data.getSpeedWindow().removeFirst();
//        double newTpointno=0;
//        if(data.getDirection()==1) {
//            newTpointno = data.getMileage() + distanceDiff; // 更新里程点
//        }else {
//            newTpointno = data.getMileage() - distanceDiff; // 更新里程点
//        }
//        if(newTpointno<mainRoadMinMillage||newTpointno>mainRoadMaxMillage)return null;
//        data.setMileage((int)newTpointno);
//        long carid = data.getId();
//        data.setSpeed(predictedSpeed);
//        data.setTimeStamp(pathTimeStamp);
//        //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
//        String sk=data.getStakeId();
//
//        //没有桩号有经纬度的情况
//        if(sk==null){
//            //用上一点的经纬度返回上一个点的location和在roadlist的第几个
//            Pair<Location, Integer> kkk = LocationOP.UseLLGetSK(data.getLatitude(), data.getLongitude(), roadKDataList);
//            //加上里程后的下一个location，也就是当前点的预测location
//            Location sec=LocationOP.UseDistanceGetThisLocation(distanceDiff,roadKDataList,kkk.getValue());
////            //the上一个点的location（因为pointmap里存的是上一个点的，现在要预测当前点的）
////            Location the=kkk.getKey();
//            //sec是下一个点的location
//            data.setStakeId(sec.getLocation());
//        }
//        //没有经纬度有桩号的情况
//        else{
//            //如果不是主路，直接输出（说明数据有问题）
//            if(!(myTools.getNString(sk,0,1)).equals("K"))return null;
//            //用当前的skateID获取下一个点的经纬度，也就是当前点的经纬度
//            Location l=LocationOP.UseSKgetLL(sk, roadKDataList,distanceDiff);
//            assert l != null;
//            data.setLatitude(l.getLatitude());
//            data.setLongitude(l.getLongitude());
//            data.setStakeId(l.getLocation());
//        }
////        mark:孝汉应==89，别的的话还得改
//        double carangle=89;
//        data.setCarAngle(carangle);
//        PathPoint p=new PathPoint(data.getDirection(), data.getId(),data.getLaneNo(),(int) newTpointno, data.getPlateNo(), data.getSpeed(),data.getTimeStamp(), data.getPlateColor(), data.getVehicleType(),data.getLongitude(),data.getLatitude(),data.getCarAngle(),data.getStakeId(),  data.getOriginalType(), data.getPlateColor());
//        updateOnePointMap(p);
//        myTools.printmergePoint(p);
//        return p;
//    }
//    private static PathPoint predicOne(PathPointData PathPointData){
//        return null;
//    }
//    //防撞，如果快碰到前车，则将其speedwindow全部-10
////    private static void deSpeedWindow(PathPointData PathPointData){
////        LinkedList<Float> speedWindow = PathPointData.getSpeedWindow();
////        ListIterator<Float> iterator = speedWindow.listIterator();
////        while (iterator.hasNext()) {
////            float originalValue = iterator.next();
////            iterator.set(originalValue - 10);
////        }
////    }
//    private static float calculateMovingAverage(LinkedList<Float> speedWindow) {
//        return (float) speedWindow.stream()
//                .mapToDouble(Float::doubleValue)
//                .average()
//                .orElse(Double.NaN);
//    }
//    private static void FirstEnterputNowMap(PathTData PathTData) throws IOException {
//        List<PathPoint> p=PathTData.getPathList();
//        for(PathPoint m:p){
//            nowMap.put(m.getId(),new Pair<>(true,0));
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
////            if(PointMap.get(Point.getId()).getMileage()==null)PointMap.remove(Point.getId());
//
//        }
//    }
//    //没有更改lastreceivedtime，因为用在预测里，没有真的接受到
//    private static void updateOnePointMap(PathPoint Point) {
//
//        PathPointData data = PointMap.compute(Point.getId(), (k,v) -> new PathPointData());
//        synchronized (data) {
//            data.getSpeedWindow().add(Point.getSpeed());
//            if (data.getSpeedWindow().size() > WINDOW_SIZE) {
//                data.getSpeedWindow().removeFirst();
//            }
//            data.setMileage(Point.getMileage());
//            data.setId(Point.getId());
//            data.setSpeed(Point.getSpeed());
//            data.setDirection(Point.getDirection());
//            data.setLatitude(Point.getLatitude());
//            data.setLongitude(Point.getLongitude());
//            data.setLaneNo(Point.getLaneNo());
//            data.setCarAngle(Point.getCarAngle());
//            data.setOriginalColor(Point.getOriginalColor());
//            data.setPlateColor(Point.getPlateColor());
//            data.setStakeId(Point.getStakeId());
//            data.setPlateNo(Point.getPlateNo());
//            data.setOriginalType(Point.getOriginalType());
//            data.setTimeStamp(Point.getTimeStamp());
//            data.setVehicleType(Point.getVehicleType());
//        }
//        if(PointMap.get(Point.getId()).getMileage()==null)PointMap.remove(Point.getId());
//
//    }
//    private static void updateOneJizhanPointMap(PathPoint Point) {
//
//        PathPointData data = JizhanPointMap.compute(Point.getId(), (k,v) -> new PathPointData());
//        synchronized (data) {
//            data.getSpeedWindow().add(Point.getSpeed());
//            if (data.getSpeedWindow().size() > WINDOW_SIZE) {
//                data.getSpeedWindow().removeFirst();
//            }
//            data.setMileage(Point.getMileage());
//            data.setId(Point.getId());
//            data.setSpeed(Point.getSpeed());
//            data.setDirection(Point.getDirection());
//            data.setLatitude(Point.getLatitude());
//            data.setLongitude(Point.getLongitude());
//            data.setLaneNo(Point.getLaneNo());
//            data.setCarAngle(Point.getCarAngle());
//            data.setOriginalColor(Point.getOriginalColor());
//            data.setPlateColor(Point.getPlateColor());
//            data.setStakeId(Point.getStakeId());
//            data.setPlateNo(Point.getPlateNo());
//            data.setOriginalType(Point.getOriginalType());
//            data.setTimeStamp(Point.getTimeStamp());
//            data.setVehicleType(Point.getVehicleType());
//        }
//    }
//    private static PathPoint PDToPP(PathPointData Point) {
//        PathPoint pathPoint = new PathPoint();
//
//        pathPoint.setMileage(Point.getMileage());
//        pathPoint.setId(Point.getId());
//        pathPoint.setSpeed(Point.getSpeed());
//        pathPoint.setDirection(Point.getDirection());
//        pathPoint.setLatitude(Point.getLatitude());
//        pathPoint.setLongitude(Point.getLongitude());
//        pathPoint.setLaneNo(Point.getLaneNo());
//        pathPoint.setCarAngle(Point.getCarAngle());
//        pathPoint.setOriginalColor(Point.getOriginalColor());
//        pathPoint.setPlateColor(Point.getPlateColor());
//        pathPoint.setStakeId(Point.getStakeId());
//        pathPoint.setPlateNo(Point.getPlateNo());
//        pathPoint.setOriginalType(Point.getOriginalType());
//        pathPoint.setVehicleType(Point.getVehicleType());
//        pathPoint.setTimeStamp(Point.getTimeStamp());
//        return pathPoint;
//    }
//}