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
import whu.edu.ljj.flink.xiaohanying.Utils.*;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;


public class buquan {
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
    private static int listLen;
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
//MergedPathData.sceneTest.1 "fiberDataTest1", "fiberDataTest2", "fiberDataTest3" 100.65.38.139:9092
    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(3);
            String brokers = args[0];
            List<String> topics  = Arrays.asList(Arrays.copyOfRange(args, 1, args.length-1));
            listLen= Integer.parseInt(args[args.length-1]);
            // 从Kafka读取数据
            DataStreamSource<String> kafkaStream = env.fromSource(buildSource(brokers, topics), WatermarkStrategy.noWatermarks(), "Kafka Source1");

//读取基站数据，返回StationStream
            DataStream<PathTData> StationStream=kafkaStream.flatMap((String jsonStr, Collector<PathTData> out)-> {
                try {
                    StationData data =null;
                    //这是基站数据
                    if(myTools.getNString(jsonStr,2,10).equals("frameNum")){
                        data = JSON.parseObject(jsonStr, StationData.class);
                        String gloTime=data.getGlobalTime();
                        temp = initCurrentTime(gloTime);
                        //问题：是不是这个globaltime,orgcode这么用吗？有问题
                        PathTData p = transStationToPathTDate(data, gloTime);
                        out.collect(p);
                    }
                } catch (Exception e) {
                    System.err.println("JSON解析失败: " + jsonStr);
                }
            }).returns(PathTData.class).keyBy(PathTData::getTime);
//            //所有的雷视数据一起接入
//            StationStream.flatMap(new FlatMapFunction<StationData, Object>() {
//                @Override
//                public void flatMap(StationData stationData, Collector<Object> collector) throws Exception {
//                    if(stationData.getTargetList()!=null&&!stationData.getTargetList().isEmpty()){
//                        for(StationTarget t:stationData.getTargetList()){
//                            //如果等于下面的那个收费站，则认为上了匝道
//                            if(t.getId().equals("下面那个收费站上匝道了")){
//                                //找出光栅中车牌对应carid
//                                nowMap.forEach((key, value) -> {
//                                    //之前在主道上开如果发现匹配的车牌号
//                                    if(PointMap.get(key).getPlateNo().equals(t.getPlateNo())){
//                                        zaMap.put(new Pair<>(key,t.getPlateNo()),"BK");
//                                        nowMap.put(key,new Pair<>(false,3));
//                                    }
//                                });
//                            }
//                            //用雷视数据车牌号遍历pointmap，从而从光栅数据获取id
//
//                            //按照id更新PointMap、JizhanPointMap
//
//                        }
//                    }
//                }
//            });

            DataStream<PathTData> rasterStream = kafkaStream
                    .flatMap((String jsonStr, Collector<PathTData> out) -> {
                        try {
//                            System.out.println(jsonStr);
                            PathTData data = null;
                            //模拟数据
                            if(myTools.getNString(jsonStr,2,4).equals("SN")) {
//                                if(myTools.getNString(jsonStr,2,10).equals("pathList")) {SN
                                data = JSON.parseObject(jsonStr, PathTData.class);
//                                System.out.println(data);
                                out.collect(data);
                            }
                        } catch (Exception e) {
                            System.err.println("JSON解析失败: " + jsonStr);
                        }
                    }).returns(PathTData.class).keyBy(PathTData::getTime);
            DataStream<PathTData> rasterStream1 = kafkaStream
                    .flatMap((String jsonStr, Collector<PathTData> out) -> {
                        try {
//                            System.out.println(jsonStr);
                            PathTData data = null;
                            //mergeddata
                            if(myTools.getNString(jsonStr,2,10).equals("pathList")) {
//                                if(myTools.getNString(jsonStr,2,10).equals("pathList")) {SN
                                data = JSON.parseObject(jsonStr, PathTData.class);
                                out.collect(data);
                            }
                        } catch (Exception e) {
                            System.err.println("JSON解析失败: " + jsonStr);
                        }
                    }).returns(PathTData.class).keyBy(PathTData::getTime);
//            DataStream<PathTData> parsedStream1=rasterStream.union(StationStream);
            DataStream<PathTData> parsedStream=rasterStream.union(rasterStream1);
            SingleOutputStreamOperator<PathTData> endPathTDataStream = parsedStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {

                @Override//5.56   33.76  86.64
                public void flatMap(PathTData pathTData, Collector<PathTData> collector) throws Exception {

                    t1 = System.currentTimeMillis();
                    List<PathPoint> list;
                    PathTData pathTData1 = initResPathTDate(pathTData);
                    temp = initCurrentTime(pathTData.getTimeStamp());
                    if (!pathTData.getPathList().isEmpty()) {
                        //问题：暂时处理基站数据，暂时不处理了，因为放到下面去了，本来想进来就直接判断是stakeid是stake然后return
//                        System.out.println(pathTData.getPathList().size());
                        if (firstEnter) firstEnterInitializePointMapAndlastMap(pathTData);else putNowDataIntoTempMap(pathTData);

                        list=dealWithLackOrNotComplete();

                        pathTData1.setPathList(list);
//                        System.out.println(list);
                        collector.collect(pathTData1);

                        //mark:防撞
                        lastMap=tempMap;

                        tempMap.clear();
                        Runtime runtime = Runtime.getRuntime();
                        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
                        double usedMemoryMB = usedMemory / (1024.0 * 1024.0);
                        System.out.printf(
                                "处理了 %d 条数据, 用时 %d ms, 占用内存: %.2f MB%n",
                                list.size(),
                                (System.currentTimeMillis() - t1),
                                usedMemoryMB
                        );
                    }//pathlist.empty

                }//flatMap

            }
            );
            writeIntoKafka(endPathTDataStream);

            env.execute("Flink completion");
        }//flink env

    }//main

    private static PathTData transStationToPathTDate(StationData data, String gloTime) {
        List<PathPoint> plist=new ArrayList<>();
        for(StationTarget s: data.getTargetList()){
            //mileage\originalType\originalColor
            int mileage=s.getEnGap();
            double lon=s.getLon();
            double lat=s.getLat();

            PathPoint pp=new PathPoint(1,s.getId(),s.getLane(),mileage , s.getPicLicense()+s.getId(),s.getSpeed(), gloTime,s.getCarColor(),s.getCarType(),lon,lat,s.getAngle(),"skate",1,1);
            plist.add(pp);
        }
        PathTData p=new PathTData(data.getTargetList().size(), temp, gloTime, data.getOrgCode() , data.getOrgCode(),plist);
        return p;
    }


    private static PathPoint predictMainRoadNextOne_UpdataPointMap(long key) {
        PathPointData data=pointMap.get(key);
        if (data == null) {
            // 可选：记录日志或处理 key 不存在的情况
            System.out.println("nullnullnull");
            return null;
        }
        ConcurrentLinkedDeque<Float> spw=data.getSpeedWindow();
        predictedSpeed=calculateMovingAverage(spw);
        spw.addLast(predictedSpeed);
        if(spw.size()>WINDOW_SIZE)spw.removeFirst();
        double newTpointno=0;
        distanceDiff = myTools.calculateDistance(predictedSpeed, 200);
        if(data.getDirection()==1) newTpointno = data.getMileage() + distanceDiff; // 更新里程点
        else newTpointno = data.getMileage() - distanceDiff; // 更新里程点

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
        return PDToPP(data);
    }
    public static List<PathPoint> dealWithLackOrNotComplete(){
        List<PathPoint> list=new ArrayList<>();
//        for (Map.Entry<Long, PathPoint> entry : tempMap.entrySet()) {
//            if (entry.getValue().getStakeId().equals("skate")) {
//                System.out.println(entry.getValue());
//                list.add(entry.getValue());
//            }
//        }
        //遍历lastMap，看是否有车没了,也就是lastMap有，tempMap目前没有
        for (Map.Entry<Long, PathPoint> entry : lastMap.entrySet()) {

            long key = entry.getKey();
            //问题：对于基站的经纬度的转化的代码放在这里
//            if(entry.getValue().getStakeId().equals("skate"))continue;
            String stake ="";
            PathPoint pp=tempMap.get(key);
            PathPoint value=new PathPoint();
            //问题：车不能出边界
            if (pp == null) {//前面有车但是当前没车
                value = predictMainRoadNextOne_UpdataPointMap(key);
            }else{
                if(pp.getStakeId()==null||pp.getStakeId().isEmpty()){//处理缺失
                    stake=getSkateID(pp);
                    value.setStakeId(stake);
                    tempMap.get(key).setStakeId(stake);
                }
                //问题：经纬度null判断
                if(pp.getLatitude()==0||pp.getLongitude()==0){

                    stake=tempMap.get(key).getStakeId();
                    TrafficEventUtils.MileageConverter converter = (pp.getDirection() == 1)
                            ? mileageConverter1 : mileageConverter2;
                    if(!stake.isEmpty()){
                        double[] d=converter.findCoordinate(stakeToMileage(stake)).getLnglat();
                        tempMap.get(key).setLatitude(d[1]);
                        tempMap.get(key).setLongitude(d[0]);
                        value.setLatitude(d[1]);
                        value.setLongitude(d[0]);
                    }
                }
                value=tempMap.get(key);
            }
            list.add(value);
            if(list.size()>=listLen)return list;
        }
        return list;
    }
    private static int stakeToMileage(String stakeId) {
        return Integer.parseInt(stakeId.split("\\+")[0].substring(1)) * 1000 + Integer.parseInt(stakeId.split("\\+")[1]);
    }

    private static String MileageToStake(int newMileage) {
        return newMileage/1000+"+"+(newMileage-(newMileage/1000*1000));
    }
    private static float calculateMovingAverage(ConcurrentLinkedDeque<Float> speedWindow) {
        return (float) speedWindow.stream()
                .mapToDouble(Float::doubleValue)
                .average()
                .orElse(Double.NaN);
    }
    private static void putNowDataIntoTempMap(PathTData pathTData){
        List<PathPoint> p=pathTData.getPathList();
        for(PathPoint m:p) tempMap.put(m.getId(),m);

    }
    private static KafkaSource<String> buildSource(String brokers, List<String> topics){
        String groupId = "flink_consumer_group";

        return KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("message.max.bytes", "16777216")
                .setProperty("max.partition.fetch.bytes", "16777216")
                .build();
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
        firstEnter = false;
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
        pathPoint.setSpeedWindow(new ConcurrentLinkedDeque<>());

        return pathPoint;
    }
    public static long initCurrentTime(String time){
        try {
            // 尝试按三位毫秒格式解析
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
            LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            // 若三位毫秒格式解析失败，尝试按两位毫秒格式解析
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SS");
            LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
            return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
    }
    public static PathTData initResPathTDate(PathTData pathTData){
        pathTimeStamp=pathTData.getTimeStamp();
        pathTime= pathTData.getTime();
        PathTData pathTData1 = new PathTData();
        pathTData.setTime(pathTime);
        pathTData.setTimeStamp(pathTimeStamp);
        pathTData.setPathNum(pathTData1.getPathNum());
        pathTData.setWaySectionId(pathTData1.getWaySectionId());
        pathTData.setWaySectionName(pathTData1.getWaySectionName());
        return pathTData1;
    }
    public static String getSkateID(PathPoint pp){
        TrafficEventUtils.StakeAssignment stakeAssign = (pp.getDirection() == 1)
                ? stakeAssign1 : stakeAssign2;
        return  stakeAssign.findInsertionIndex(pp.getLongitude(), pp.getLatitude());

    }
    public static void writeIntoKafka(SingleOutputStreamOperator<PathTData> endPathTDataStream){
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

        DataStream<String> jsonStream = endPathTDataStream
                .map(JSON::toJSONString)
                .returns(String.class);

        jsonStream.sinkTo(dealStreamSink);
    }
}//public buquan class