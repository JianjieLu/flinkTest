package whu.edu.moniData.BuQuan;

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
import whu.edu.ljj.flink.utils.JsonReader;
import whu.edu.ljj.flink.utils.myTools;
import static whu.edu.ljj.flink.xiaohanying.Utils.*;
import whu.edu.moniData.Utils.TrafficEventUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

import static whu.edu.ljj.flink.utils.LocationOP.UseSKgetLL;
import static whu.edu.ljj.flink.utils.calAngle.calculateBearing;


public class buquanji {
    private static final int WINDOW_SIZE = 20;//用来预测的窗口大小
    private static final Map<Long, PathPointData> pointMap = new ConcurrentHashMap<>();
    static boolean firstEnter = true;
    static Map<Long, PathPoint> tempMap = new ConcurrentHashMap<>();
    private static String pathTimeStamp = "";
    private static long pathTime = 0;
    private static long temp = 0;
    private static TrafficEventUtils.MileageConverter mileageConverter1;
    private static TrafficEventUtils.MileageConverter mileageConverter2;
    private static TrafficEventUtils.StakeAssignment stakeAssign1;
    private static TrafficEventUtils.StakeAssignment stakeAssign2;
    static List<Location> roadAKDataList;
    static List<Location> roadBKDataList;
    static List<Location> roadCKDataList;
    static List<Location> roadDKDataList;
    static long currentTimeMillis=0;

    static {
        try {
            mileageConverter1 = new TrafficEventUtils.MileageConverter("sx_json.json");
            mileageConverter2 = new TrafficEventUtils.MileageConverter("xx_json.json");
            stakeAssign1 = new TrafficEventUtils.StakeAssignment("sx_json.json");
            stakeAssign2 = new TrafficEventUtils.StakeAssignment("xx_json.json");
            roadAKDataList = JsonReader.readJsonFile("AK_locations.json");
            roadBKDataList = JsonReader.readJsonFile("BK_locations.json");
            roadCKDataList = JsonReader.readJsonFile("CK_locations.json");
            roadDKDataList = JsonReader.readJsonFile("DK_locations.json");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //        60817                        62914                         62914
    //生产者端max.request.size必须小于集群的message.max.bytes以及消费者的max.partition.fetch.bytes
    //MergedPathData.sceneTest.1 "fiberDataTest1", "fiberDataTest2", "fiberDataTest3" 100.65.38.139:9092
    public static void main(String[] args) throws Exception {

        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(3);
            String brokers = args[0];
            List<String> topics  = Arrays.asList("MergedPathData");
            String groupId = "flink_consumer_group1";
            // 从Kafka读取数据
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
//                    .setProperty("max.request.size", "608174080")
                    .setProperty("max.partition.fetch.bytes", "629145600")
                    .build();
            // 从Kafka读取数据
//            DataStreamSource<String> kafkaStream = env.fromSource(buildSource(brokers, topics), WatermarkStrategy.noWatermarks(), "Kafka Source1");
            DataStreamSource<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source1");
//读取基站数据，返回StationStream
            DataStream<PathTData> StationStream=kafkaStream.flatMap((String jsonStr, Collector<PathTData> out)-> {
                try {
                    currentTimeMillis=System.currentTimeMillis();
                    PathTData data1 = null;
//                    if(myTools.getNString(jsonStr,2,10).equals("pathList")) {
                    data1 = JSON.parseObject(jsonStr, PathTData.class);
                    for(PathPoint p:data1.getPathList()){
                        p.setStakeId(p.getStakeId().split("-")[1]);
                    }
                    out.collect(data1);
//                    }
                } catch (Exception e) {
                    System.err.println("JSON解析失败: "+ e.getMessage());
                }
            }).returns(PathTData.class).keyBy(PathTData::getTime);
            SingleOutputStreamOperator<PathTData> endPathTDataStream = StationStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {
                @Override//5.56   33.76  86.64
                public void flatMap(PathTData pathTData, Collector<PathTData> collector) throws Exception {
                    long t1 = System.currentTimeMillis();

                    List<PathPoint> list = new ArrayList<>();
                    PathTData pathTData1 = initResPathTDate(pathTData);
                    String ts = pathTData.getTimeStamp();
                    temp = initCurrentTime(ts);
//                    System.out.println(pathTData.getTimeStamp());
                    if (!pathTData.getPathList().isEmpty()) {
                        if (firstEnter) {
                            list = firstEnterInitializePointMap(pathTData);
                            pathTData1.setPathList(list);
                        } else {

                            putNowDataIntoTempMap(pathTData, ts);

                            for (Map.Entry<Long, PathPoint> entry : tempMap.entrySet()) {//当前的所有数据直接加入
                                PathPoint p = entry.getValue();
                                if (p.getStakeId() != null && p.getTimeStamp() != null) list.add(p);

                            }
//                        }
//                    }
//                                            pathTData1.setPathList(list);
//                        collector.collect(pathTData1);
//                        tempMap.clear();
//                }
//
//                            System.out.println("tempMap.size():  "+tempMap.size()+"  content:"+tempMap);

                            //如果里程越界，会被移除
                            for (Map.Entry<Long, PathPointData> entry : pointMap.entrySet()) {
                                if (tempMap.get(entry.getKey()) == null) {//PointMap中有，但是当前tempMap中没有，车辆缺失
                                    PathPointData pdInPointMap = predictNextMixed(entry.getKey(), pathTData.getTimeStamp());
                                    if (pdInPointMap != null) {

                                        list.add(PDToPP(pdInPointMap));
                                        pointMap.put(pdInPointMap.getId(), pdInPointMap);
                                    }else{
                                        list.add(new PathPoint(0,entry.getKey(),0,0,"0",0,"0",0,0,0,0,0,"0",0,0,0,new eventInfo()));
                                    }
                                }
                            }
                            //更新pointmap
//                            for(PathPoint p:pathTData.getPathList()) {
//                                long keyId=p.getId();
//                                if (pointMap.get(keyId) == null) {//即前面没有，当前有，是个新车。（会不会是重新出现的车呢）反正在这个if里是个绝对的新车
//                                    PathPointData ppp = PPToPDAndinitLastRecAndWindow(p);
//                                    pointMap.put(keyId, ppp);
//                                } else {//pointmap有，当前有，看前一条是不是预测的
//                                    if(  pointMap.get(keyId).getLastReceivedTime() ==1) {//说明前面是预测的
//                                        //重新出现，怎么处理？把前面的先全部删掉,然后再全部加入。那么会不会跟前面的《当前的所有数据直接加入》重复呢？不会，前面的没有改变pointmap，只是加入list
//                                        pointMap.remove(keyId);
//                                        PathPointData pdpd= PPToPD(p);
//                                        ConcurrentLinkedDeque<Float> c = new ConcurrentLinkedDeque<>();
//                                        c.add(p.getSpeed());
//                                        pdpd.setSpeedWindow(c);
//                                        pdpd.setLastReceivedTime(0);
//                                        pointMap.put(keyId,pdpd);
//                                    }else{//前面有，当前有，且前面的不是预测的
//                                        pointMap.get(keyId).getSpeedWindow().add(tempMap.get(keyId).getSpeed());
//                                    }
//                                }
//
//                            }

                        }
                        pathTData1.setPathList(list);
                        collector.collect(pathTData1);
                        tempMap.clear();
                        System.out.println("list.size():  "+list.size());
                        Set<Long> se=new HashSet<>();
                        List<String >s=new ArrayList<>();
//                        for(PathPoint p:list){
//                            se.add(p.getId());
////                            System.out.println(p);
//                            s.add(p.getPlateNo()+"  "+p.getStakeId());
//                        }
//                        System.out.println("se.size():  "+se.size());
                        System.out.println(s);
                    }//pathlist.empty
                    long t2 = System.currentTimeMillis();
                    System.out.println("neibu:"+(t2-t1)+"  waibu:"+(t2- currentTimeMillis));
                }//flatMap

            }
            );

            writeIntoKafka(endPathTDataStream);
            System.out.println("ok:"+(System.currentTimeMillis()-currentTimeMillis));
            env.execute("Flink completion to Kafkaji : completed.pathdata");
        }//flink env

    }//main
    private static PathPointData newPD(PathPoint pp){
        PathPointData pd=PPToPD(pp);
        pd.getSpeedWindow().add(pp.getSpeed());
        return pd;
    }
    private static PathPointData predictNextMixed(long keyInPointMap,String timestamp){
        PathPointData pdInPointMap=pointMap.get(keyInPointMap);
        Pair<ConcurrentLinkedDeque<Float>,Float> a1=predictSpeedWindow(pdInPointMap);//速度窗口、预测的速度
        double[] a2=predictNewMileage(pdInPointMap,a1.getValue());//新里程、驶过的距离
        Pair<String,double[]> a3=predictStake(pdInPointMap,a2[0],a2[1]);//新桩号、新经纬度lonlng
        if(a3==null) {
            System.out.println("已移除 "+pdInPointMap.getId());
            return null;
        }
        if(a3.getValue()[1]==pdInPointMap.getLatitude())return null;
        double carangle=calculateBearing(a3.getValue()[1],a3.getValue()[0],pdInPointMap.getLatitude(),pdInPointMap.getLongitude());
        pdInPointMap.setCarAngle(carangle);
        pdInPointMap.setMileage((int) (a2[0]));
        pdInPointMap.setSpeed(a1.getValue());
//        pdInPointMap.setTimeStamp(pathTimeStamp);//未接收到，不更新
        pdInPointMap.setLatitude(a3.getValue()[1]);
        pdInPointMap.setLongitude(a3.getValue()[0]);
        pdInPointMap.setSpeedWindow(a1.getKey());
        pdInPointMap.setStakeId(a3.getKey());
        pdInPointMap.setTimeStamp(timestamp);
        if(!pdInPointMap.getPlateNo().endsWith("值"))pdInPointMap.setPlateNo(pdInPointMap.getPlateNo()+" "+"预测值");
        else{
            pdInPointMap.setPlateNo(pdInPointMap.getPlateNo().substring(0,7)+" "+"预测值");
        }
        PathPoint pp=PDToPP(pdInPointMap);
        System.out.println("enter yvce"+pdInPointMap.getId()+"  "+ pdInPointMap.getPlateNo());

        return pdInPointMap;
    }
//    private static PathTData transStationToPathTDate(StationData data, String gloTime) {
//        List<PathPoint> plist=new ArrayList<>();
//        for(StationTarget s: data.getTargetList()){
//            //mileage\originalType\originalColor
//            int mileage=s.getEnGap();
//            double lon=s.getLon();
//            double lat=s.getLat();
//
//            PathPoint pp=new PathPoint(1,s.getId(),s.getLane(),mileage , s.getPicLicense()+"================",s.getSpeed(), gloTime,s.getCarColor(),s.getCarType(),lon,lat,s.getAngle(),"skate",1,1);
//            plist.add(pp);
//        }
//        PathTData p=new PathTData(data.getTargetList().size(), temp, gloTime, data.getOrgCode() , data.getOrgCode(),plist);
//        return p;
//    }
    //distance：新的总里程，deta：驶过的距离
    public static Pair<String,double[]> predictStake(PathPointData data,double distance,double deta){
        String lastStake=data.getStakeId();
        char a=lastStake.charAt(0);
        String newStake="";
        System.out.println("last:"+lastStake+"   deta:"+deta+"  id:"+data.getId());
        double[]d = new double[2];
        if(a=='A'){
            newStake="AK"+MileageToStake((int)distance);
            Location l=UseSKgetLL(lastStake, roadAKDataList, deta,4161);
            if(l==null){
                pointMap.remove(data.getId());tempMap.remove(data.getId());
                return null;
            }else{
                d[0]= l.getLongitude();
                d[1]= l.getLatitude();
            }

        }else if(a=='B'){
            newStake="BK"+MileageToStake((int)distance);
            Location l=UseSKgetLL(lastStake, roadBKDataList, deta,4309);
            if(l==null){pointMap.remove(data.getId());tempMap.remove(data.getId());

                return null;
            }else{
                d[0]= l.getLongitude();
                d[1]= l.getLatitude();
            }

        }else if(a=='C'){
            newStake="CK"+MileageToStake((int)distance);
            Location l=UseSKgetLL(lastStake, roadCKDataList, deta,779);
            if(l==null){pointMap.remove(data.getId());tempMap.remove(data.getId());

                return null;
            }else{
                d[0]= l.getLongitude();
                d[1]= l.getLatitude();
            }
        }else if(a=='D'){
            newStake="DK"+MileageToStake((int)distance);
            Location l=UseSKgetLL(lastStake, roadDKDataList, deta,732);
            if(l==null){pointMap.remove(data.getId());tempMap.remove(data.getId());

                return null;
            }else{
                d[0]= l.getLongitude();
                d[1]= l.getLatitude();
            }
        }else if(a=='K'){
            newStake="K"+MileageToStake((int)distance);
            TrafficEventUtils.MileageConverter converter = (data.getDirection() == 1) ? mileageConverter1 : mileageConverter2;
            d=converter.findCoordinate((int) distance).getLnglat();
        }
        return new Pair<>(newStake,d);
    }
    public static double[] predictNewMileage(PathPointData data,float speed){

        double[]d={0,0};
        d[1] = myTools.calculateDistance(speed, 200);
        d[0] = data.getMileage() + d[1]; // 更新里程点

        //问题：新里程是否过大
        return d;
    }
    public static Pair<ConcurrentLinkedDeque<Float>,Float> predictSpeedWindow(PathPointData data){
        ConcurrentLinkedDeque<Float> spw = data.getSpeedWindow();
        float predictedSpeed = spw.isEmpty() ? data.getSpeed() : calculateMovingAverage(spw);

        // 确保窗口操作的原子性
        ConcurrentLinkedDeque<Float> newWindow = new ConcurrentLinkedDeque<>(spw);
        newWindow.add(predictedSpeed);
        if (newWindow.size() > WINDOW_SIZE) {
            newWindow.poll();
        }

        return new Pair<>(newWindow, predictedSpeed);
    }
    private static String MileageToStake(int newMileage) {
        return newMileage/1000+"+"+(newMileage-(newMileage/1000*1000));
    }
    private static float calculateMovingAverage(ConcurrentLinkedDeque<Float> speedWindow) {
        if (speedWindow.isEmpty()) return 0.0f;

        double sum = 0;
        int count = 0;
        for (Float speed : speedWindow) {
            if (speed != null) {
                sum += speed;
                count++;
            }
        }
        return (count > 0) ? (float)(sum / count) : 0.0f;
    }
    private static void putNowDataIntoTempMap(PathTData pathTData,String time){
        List<PathPoint> p=pathTData.getPathList();
        for(PathPoint m:p){
            m.setTimeStamp(time);
            tempMap.put(m.getId(),m);
        }
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
    private static List<PathPoint> firstEnterInitializePointMap (PathTData pathTData){
        List<PathPoint> p=pathTData.getPathList();
        for(PathPoint m:p){
            PathPointData pp=PPToPD(m);
            pp.setLastReceivedTime(pathTData.getTime());
            pp.getSpeedWindow().add(m.getSpeed());
            pointMap.put(m.getId(),pp);
        }
        firstEnter = false;
        return p;
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
                .setBootstrapServers("100.65.38.40:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("completed.pathdata")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setProperty("max.request.size", "20971520") // 20MB
                .build();

        DataStream<String> jsonStream = endPathTDataStream
                .map(JSON::toJSONString)
                .returns(String.class);

        jsonStream.sinkTo(dealStreamSink);
    }
}//public buquan class