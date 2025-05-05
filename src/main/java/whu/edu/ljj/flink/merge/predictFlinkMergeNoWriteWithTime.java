package whu.edu.ljj.flink.merge;

import com.alibaba.fastjson2.JSON;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import whu.edu.ljj.flink.utils.JsonReader;
import whu.edu.ljj.flink.utils.LocationOP;
import whu.edu.ljj.flink.utils.myTools;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static whu.edu.ljj.flink.xiaohanying.Utils.*;

public class predictFlinkMergeNoWriteWithTime {
    private static final int WINDOW_SIZE = 20;//用来预测的窗口大小
    private static final Map<Long, PathPointData> PointMap = new ConcurrentHashMap<>();
    private static final Map<Long, PathPointData> JizhanPointMap = new ConcurrentHashMap<>();//雷视数据获取到的匝道上的所有车
    static boolean firstEnter=true;
    static Map<Long,Pair<Boolean,Integer>> nowMap= new ConcurrentHashMap<>();
    //    carid  是否在路上  数据缺失了几次
    static Map<Pair<Long,String>,String> zaMap= new ConcurrentHashMap<>();
    //       carid  carNumber  匝道编号
    private static final long mainRoadMinMillage=0;//主路上的最小里程
    private static final long mainRoadMaxMillage=1111111111;//主路上的最大里程
    static List<Location> roadKDataList;
    static List<Location> roadAKDataList;
    static List<Location> roadBKDataList;
    static List<Location> roadCKDataList;
    static List<Location> roadDKDataList;
    private static String pathTimeStamp="";
    private static float predictedSpeed=0;//预测速度
    private static double distanceDiff=0;
    private static long pathTime=0;
    private static int tcount=0;
    private static long t1=0;
    private static boolean tb1=true;
    private static boolean tb2=true;
    private static long t2=0;
    private static int newscount=0;

    static {
        try {
            roadKDataList  = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\K_locations.json");
            roadAKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\AK_locations.json");
            roadBKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\BK_locations.json");
            roadCKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\CK_locations.json");
            roadDKDataList = JsonReader.readJsonFile("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\zadaoGeojson\\DK_locations.json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void main(String[] args) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(3);
            // 配置Kafka连接信息
            String brokers = "100.65.38.40:9092";
            String groupId = "flink_consumer_group";
            List<String> topics = Arrays.asList("MergedPathData");
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


            DataStream<TrackObject> TrackStream=kafkaStream.flatMap((String jsonStr, Collector<TrackObject> out)-> {
                try {
                    TrackObject data =null;
                    //验证，如果json的前几位是timestamp，则认为是mergedata
                    if(myTools.getNString(jsonStr,2,11).equals("vehicleTrackID")){
                        data = JSON.parseObject(jsonStr, TrackObject.class);
                        out.collect(data);
                    }
                } catch (Exception e) {
                    System.err.println("JSON解析失败: " + jsonStr);
                }
            }).returns(TrackObject.class).keyBy(TrackObject::getId);
            //所有的雷视数据一起接入
            TrackStream.flatMap(new FlatMapFunction<TrackObject, Object>() {
                @Override
                public void flatMap(TrackObject trackObject, Collector<Object> collector) throws Exception {
                    if(trackObject.getTracks()!=null&&!trackObject.getTracks().isEmpty()){
                        for(Track t:trackObject.getTracks()){
                            //如果等于下面的那个收费站，则认为上了匝道
                            if(t.getTrackID().equals("下面那个收费站上匝道了")){
                                //找出光栅中车牌对应carid
                                nowMap.forEach((key, value) -> {
                                    //之前在主道上开如果发现匹配的车牌号
                                    if(PointMap.get(key).getPlateNo().equals(t.getPlateNo())){
                                        zaMap.put(new Pair<>(key,t.getPlateNo()),"BK");
                                        nowMap.put(key,new Pair<>(false,3));
                                    }
                                });
                            }
                            //用雷视数据车牌号遍历pointmap，从而从光栅数据获取id

                            //按照id更新PointMap、JizhanPointMap

                        }
                    }
                }
            });


            DataStream<PathTData> parsedStream = kafkaStream
                    .flatMap((String jsonStr, Collector<PathTData> out) -> {
                        try {
                            PathTData data =null;
                            //验证，如果json的前几位是timestamp，则认为是mergedata
                            if(myTools.getNString(jsonStr,2,11).equals("timeStamp")){
                                data = JSON.parseObject(jsonStr, PathTData.class);
//                                try (BufferedWriter writer1 = new BufferedWriter(new FileWriter("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\mergedata\\test.txt",true))) {
//                                    writer1.write(jsonStr);
//                                    writer1.write(System.lineSeparator());
                                }

                                out.collect(data);
//                            }
                        } catch (Exception e) {
                            System.err.println("JSON解析失败: " + jsonStr);
                        }
                    }).returns(PathTData.class).keyBy(PathTData::getTime);
            SingleOutputStreamOperator<PathTData> endPathTDataStream=parsedStream.flatMap(new FlatMapFunction<PathTData, PathTData>() {
                @Override//5.56   33.76  86.64
                public void flatMap(PathTData PathTData, Collector<PathTData> collector) throws Exception {
//                    System.out.println(myTools.toDateTimeString(System.currentTimeMillis()));

                        t1 = System.currentTimeMillis();

                    pathTimeStamp=PathTData.getTimeStamp();
                    pathTime= PathTData.getTime();
                    PathTData pathTData = new PathTData();
                    pathTData.setTime(pathTime);
                    pathTData.setTimeStamp(pathTimeStamp);
                    pathTData.setPathNum(PathTData.getPathNum());
                    pathTData.setWaySectionId(PathTData.getWaySectionId());
                    pathTData.setWaySectionName(PathTData.getWaySectionName());
                    List<PathPoint> list=new ArrayList<>();
                    //如果mergedata合法
                    if (!PathTData.getPathList().isEmpty()) {
                        //存车辆id对应的车辆是否在车道上、几次没有出现
                        Map<Long,Pair<Boolean,Integer>> tempMap= new ConcurrentHashMap<>();

//                        b=true;
                        updateMergePoint(PathTData);//更新当前车辆map
                        List<PathPoint> p=PathTData.getPathList();
                            for (PathPoint m : p) {
//                                try (BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\mergedata\\result\\data2\\"+m.getId()+".txt",true))) {
//                                    writer.write("ID:"+m.getId()+" SKID:"+m.getStakeId()+"  timeStamp:"+m.getTimeStamp());
//                                    writer.write(System.lineSeparator());
//                                }
                                if (firstEnter) {//第一次有数据，初始化nowmap
                                    FirstEnterputNowMap(PathTData);
                                    tempMap = nowMap;
                                    firstEnter = false;
                                } else {
                                    tempMap.put(m.getId(), new Pair<>(true, 0));
                                }
                            }
//                            try (BufferedWriter writer = new BufferedWriter(new FileWriter("D:\\learn\\codes\\a_idea_codes\\flinkTest\\src\\main\\java\\whu\\edu\\ljj\\flink\\data\\mergedata\\my.txt",true))) {
                            newscount++;
//                            tempMap.forEach((key, value) -> {
//                                if(myTools.getNString(PointMap.get(key).getStakeId(),0,1).equals("AK")){
//
//                                };
//                            });
                            //不是第一次有数据，已经初始化nowmap，遍历nowmap，看是否有车没了,也就是nowmap有，目前没有
                            //里程+数据丢失检测 上匝道
                        for (Map.Entry<Long, Pair<Boolean,Integer>> entry : nowMap.entrySet()) {
                            long key = entry.getKey();
                            Pair<Boolean, Integer> now = nowMap.get(key);
                            PathPointData pathPointData = PointMap.get(key);
                            if(pathPointData!=null) {
                                if (tempMap.get(key) == null && pathPointData.getMileage() != null) {//如果当前这辆车目前没有但是nowmap有，视为可能缺失。
                                    // 上面pathPointData.getMileage()!=null是因为模拟数据有的没mileage
                                    if (now.getKey()) {//==true就是还是三次以下 ==false就是已经消失三次以上，视为没了
                                        tempMap.put(key, new Pair<>(true, now.getValue() + 1));
                                        if (tempMap.get(key).getValue() == 1) {
                                            tempMap.put(key, new Pair<>(false, now.getValue()));
                                            if (pathPointData.getMileage() >= 1121970 && pathPointData.getMileage() <= 1121990 && pathPointData.getDirection() == 1) {
                                                zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "CK");
                                            } else if (pathPointData.getMileage() >= 1122544 && pathPointData.getMileage() <= 1122564 && pathPointData.getDirection() == 2) {
                                                zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "AK");
                                            }
//                                        mark:移出zaMap的逻辑
                                        }
                                    } else tempMap.put(key, new Pair<>(false, now.getValue() + 1));
                                }
                            }
                        }
                        PointMap.forEach((k,v)->{
                            if(v.getMileage()==null)PointMap.remove(k);
                        });
                        JizhanPointMap.forEach((k,v)->{
                            if(v.getMileage()==null)JizhanPointMap.remove(k);
                        });
                        for (Map.Entry<Long, Pair<Boolean,Integer>> entry : tempMap.entrySet()) {
                            long key =entry.getKey();
                            PathPointData pathPointData =PointMap.get(key);
                            if(pathPointData!=null) {
                                if (pathPointData.getStakeId() != null) {
                                    String nString = myTools.getNString(pathPointData.getStakeId(), 0, 2);
                                    switch (nString) {
                                        case "AK":
                                            zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "AK");
                                            tempMap.put(key,new Pair<>(false,3));
//                                            System.out.println(zaMap.get(new Pair<>(key, pathPointData.getPlateNo())));
                                            break;
                                        case "BK":
                                            zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "BK");
                                            tempMap.put(key,new Pair<>(false,3));
                                            break;
                                        case "CK":
                                            zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "CK");
                                            tempMap.put(key,new Pair<>(false,3));
                                            break;
                                        case "DK":
                                            zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "DK");
                                            tempMap.put(key,new Pair<>(false,3));

                                            break;
                                        case "K":
                                            if(zaMap.get(new Pair<>(key, pathPointData.getPlateNo()))!=null){
                                                zaMap.remove(new Pair<>(key, pathPointData.getPlateNo()));
                                                tempMap.put(key,new Pair<>(true,0));
                                                nowMap.put(key,new Pair<>(true,0));
                                            }
                                            break;
                                    }
                                }
                            }
                        }
                        for (Map.Entry<Long, PathPointData> entry : JizhanPointMap.entrySet()) {
                            long key =entry.getKey();
                            PathPointData pathPointData =PointMap.get(key);
                            if(pathPointData!=null) {
                                String nString = myTools.getNString(PointMap.get(key).getStakeId(), 0, 2);
                                switch (nString) {
                                    case "AK":
                                        zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "AK");
                                        break;
                                    case "BK":
                                        zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "BK");
                                        break;
                                    case "CK":
                                        zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "CK");
                                        break;
                                    case "DK":
                                        zaMap.put(new Pair<>(key, pathPointData.getPlateNo()), "DK");
                                        break;
                                    case "K":
                                        if(zaMap.get(new Pair<>(key, pathPointData.getPlateNo()))!=null){
                                            zaMap.remove(new Pair<>(key, pathPointData.getPlateNo()));
                                            tempMap.put(key,new Pair<>(true,0));
                                            nowMap.put(key,new Pair<>(true,0));
                                        }
                                        break;
                                }
                            }
                        }
                        tempMap.forEach((key, value) -> {
                        PathPointData pd= PointMap.get(key);
                        if(pd!=null) {
                            //预测的点要加上：mileage、speed、skateID
                            if (!value.getKey()) {//不在路上
//                                if(pd.getSpeedWindow()==null)System.out.println(pd);
                                predictedSpeed = calculateMovingAverage(pd.getSpeedWindow());
                                distanceDiff = myTools.calculateDistance(predictedSpeed, 250); // 米
                                if (zaMap.get(new Pair<>(key, pd.getPlateNo())) != null) {//id对应的zamap不为null，为上了匝道
//                                    try {
//                                        writer.write("zamap != null:"+zaMap.get(new Pair<>(key, pd.getPlateNo())));
//                                    } catch (IOException e) {
//                                        throw new RuntimeException(e);
//                                    }
                                    try {
                                        PathPointData jizhanpoint = JizhanPointMap.get(key);//先看看雷视数据里有没有数据，有就用雷视，没有就预测
                                        if (jizhanpoint != null) {
                                            if (System.currentTimeMillis() - jizhanpoint.getLastReceivedTime() < 300) {//相差少于300秒，数据没有缺失，输出雷视数据
                                                //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
                                                String sk = jizhanpoint.getStakeId();
                                                //如果雷视数据的桩号为null，则计算出经纬度，如果不为null,则通过经纬度计算出桩号
                                                if (sk == null) {
                                                    //上一个点的location（因为pointmap里存的是上一个点的，现在要预测当前点的）
                                                    Pair<Location, Integer> kkk = null;
                                                    try {
                                                        kkk = LocationOP.UseLLGetSK(jizhanpoint.getLatitude(), jizhanpoint.getLongitude(), roadKDataList);
                                                    } catch (IOException e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                    jizhanpoint.setStakeId(kkk.getKey().getLocation());
                                                } else {
                                                    //question
                                                    if (jizhanpoint.getLatitude() == 0) {
                                                        //如果匝道号为"K",则应该在匝道，桩号却在主路上，说明数据有问题
                                                        if ((myTools.getNString(sk, 0, 1)).equals("K")) return;
                                                        Location l = LocationOP.UseSKgetLL(sk, roadKDataList, distanceDiff);
                                                        assert l != null;
                                                        jizhanpoint.setLatitude(l.getLatitude());
                                                        jizhanpoint.setLongitude(l.getLongitude());
                                                    }
                                                }
                                                PathPoint pathPoint = PDToPP(jizhanpoint);
                                                list.add(pathPoint);
                                            }
                                        } else {//jizhanpoint == null
                                            PathPoint pathPoint = zhadaoPredictNextOne(pd);
                                            updateOneJizhanPointMap(pathPoint);
                                            updateOnePointMap(pathPoint);
                                            list.add(pathPoint);
                                        }
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                } else {//没上匝道但数据断连
                                    try {
                                        PathPoint pathPoint = PredictNextOne(pd);
                                        if (pathPoint != null) {
                                            list.add(pathPoint);
                                        }

                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            } else {//if (value.getKey()) {//在路上

                                //因为数据正确，进来的时候就已经改了pointmap，无需再改
                                //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
                                String sk = pd.getStakeId();
                                //如果pd的桩号为null，则计算出经纬度，如果不为null,则通过经纬度计算出桩号
                                if (sk == null) {
                                    //上一个点的location（因为pointmap里存的是上一个点的，现在要预测当前点的）
                                    Pair<Location, Integer> kkk = null;
                                    try {kkk = LocationOP.UseLLGetSK(pd.getLatitude(), pd.getLongitude(), roadKDataList);} catch (IOException e) {throw new RuntimeException(e);}
                                    pd.setStakeId(kkk.getKey().getLocation());
                                } else {//sk !== null
                                    //question
                                    if (pd.getLatitude() == 0) {
                                        //应该在主路，桩号却在匝道上，说明数据有问题
                                        if (!(myTools.getNString(sk, 0, 1)).equals("K")) return;
                                        Location l = LocationOP.UseSKgetLL(sk, roadKDataList, distanceDiff);
                                        assert l != null;
                                        pd.setLatitude(l.getLatitude());
                                        pd.setLongitude(l.getLongitude());
                                    }
                                }
                                PathPoint pathPoint = PDToPP(pd);
                                list.add(pathPoint);
                                myTools.printPathPoint(pathPoint);
                            }//if (value.getKey()) {//在路上
                        }//tempmap的key去取pointmap不为null，最大那个没了

                                });
                        //整个writter
                        pathTData.setPathList(list);
                        collector.collect(pathTData);
                        //mark:防撞
                        nowMap=tempMap;
                    }

                        t2 = System.currentTimeMillis();
                        System.out.println("time split:"+(t2-t1));
                }

            });
//            // 执行任务
            env.execute("Flink Read Kafka");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    //匝道上的预测
    private static PathPoint zhadaoPredictNextOne(PathPointData data) throws IOException {
    long currentTime = System.currentTimeMillis();
    PathPoint p = null;
    //经过的时间少于某一值（设置时间限制，避免一直计算预测）
//    if (currentTime-data.getLastReceivedTime()<80000000) {
        // 使用车辆独立窗口计算
        data.getSpeedWindow().addLast(predictedSpeed);
        data.getSpeedWindow().removeFirst();
        double newTpointno=0;
        if(data.getDirection()==1) newTpointno = data.getMileage() + distanceDiff; // 更新里程点
        else newTpointno = data.getMileage() - distanceDiff; // 更新里程点
        data.setMileage((int)newTpointno);
        long carid = data.getId();
        data.setSpeed(predictedSpeed);
        data.setTimeStamp(pathTimeStamp);
        //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
        String sk=data.getStakeId();
        Location the =new Location();
        Location sec = new Location();
        String whichk=zaMap.get((new Pair<>(carid, data.getPlateNo())));
        //当前点的location
        if(sk.isEmpty()) {
            //下一个点的location及位于第几个
            Pair<Location, Integer> kkk =null;
            switch (whichk) {
                case "AK":
                    kkk= LocationOP.UseLLGetSK(data.getLatitude(), data.getLongitude(), roadAKDataList);
                    the = kkk.getKey();
                    sec = LocationOP.UseDistanceGetThisLocation(distanceDiff, roadAKDataList, kkk.getValue());
                    break;
                case "BK":
                    kkk= LocationOP.UseLLGetSK(data.getLatitude(), data.getLongitude(), roadBKDataList);
                    the = kkk.getKey();
                    sec = LocationOP.UseDistanceGetThisLocation(distanceDiff, roadBKDataList, kkk.getValue());
                    break;
                case "CK":
                    kkk= LocationOP.UseLLGetSK(data.getLatitude(), data.getLongitude(), roadCKDataList);
                    the = kkk.getKey();
                    sec = LocationOP.UseDistanceGetThisLocation(distanceDiff, roadCKDataList, kkk.getValue());
                    break;
                case "DK":
                    kkk= LocationOP.UseLLGetSK(data.getLatitude(), data.getLongitude(), roadDKDataList);
                    the = kkk.getKey();
                    sec = LocationOP.UseDistanceGetThisLocation(distanceDiff, roadDKDataList, kkk.getValue());
                    break;
            }
            data.setStakeId(sec.getLocation());
        }
        else{
            switch (whichk) {
                case "AK": {
                    Location l = LocationOP.UseSKgetLL(sk, roadAKDataList,distanceDiff);
                    assert l != null;
                    data.setLatitude(l.getLatitude());
                    data.setLongitude(l.getLongitude());
                    break;
                }
                case "BK": {
                    Location l = LocationOP.UseSKgetLL(sk, roadBKDataList,distanceDiff);
                    assert l != null;
                    data.setLatitude(l.getLatitude());
                    data.setLongitude(l.getLongitude());
                    break;
                }
                case "CK": {
                    Location l = LocationOP.UseSKgetLL(sk, roadCKDataList,distanceDiff);
                    assert l != null;
                    data.setLatitude(l.getLatitude());
                    data.setLongitude(l.getLongitude());
                    break;
                }
                case "DK": {
                    Location l = LocationOP.UseSKgetLL(sk, roadDKDataList,distanceDiff);
                    assert l != null;
                    data.setLatitude(l.getLatitude());
                    data.setLongitude(l.getLongitude());
                    break;
                }
            }
        }
        double carangle=myTools.calculateBearing(the.getLatitude(),the.getLongitude(),sec.getLatitude(),sec.getLongitude());
        //mark:==0还有问题
        if(data.getCarAngle()==0)data.setCarAngle(carangle);
        p=new PathPoint(data.getDirection(), data.getId(),data.getLaneNo(),(int) newTpointno, data.getPlateNo(), data.getSpeed(),data.getTimeStamp(), data.getPlateColor(), data.getVehicleType(),data.getLongitude(),data.getLatitude(),data.getCarAngle(),data.getStakeId(),  data.getOriginalType(), data.getPlateColor());
        myTools.printmergePoint(p);
//    }

        updateOnePointMap(p);
//        myTools.printmergePoint(p);
    return p;
}
    //主路上的预测
    private static PathPoint PredictNextOne(PathPointData data) throws IOException {
        data.getSpeedWindow().addLast(predictedSpeed);
        data.getSpeedWindow().removeFirst();
        double newTpointno=0;
        if(data.getDirection()==1) {
            newTpointno = data.getMileage() + distanceDiff; // 更新里程点
        }else {
            newTpointno = data.getMileage() - distanceDiff; // 更新里程点
        }
        if(newTpointno<mainRoadMinMillage||newTpointno>mainRoadMaxMillage)return null;
        data.setMileage((int)newTpointno);
        long carid = data.getId();
        data.setSpeed(predictedSpeed);
        data.setTimeStamp(pathTimeStamp);
        //如果StakeId为空，则计算经纬度，如果经纬度为空，则计算stakeid
        String sk=data.getStakeId();

        //没有桩号有经纬度的情况
        if(sk==null){
            //用上一点的经纬度返回上一个点的location和在roadlist的第几个
            Pair<Location, Integer> kkk = LocationOP.UseLLGetSK(data.getLatitude(), data.getLongitude(), roadKDataList);
            //加上里程后的下一个location，也就是当前点的预测location
            Location sec=LocationOP.UseDistanceGetThisLocation(distanceDiff,roadKDataList,kkk.getValue());
//            //the上一个点的location（因为pointmap里存的是上一个点的，现在要预测当前点的）
//            Location the=kkk.getKey();
            //sec是下一个点的location
            data.setStakeId(sec.getLocation());
        }
        //没有经纬度有桩号的情况
        else{
            //如果不是主路，直接输出（说明数据有问题）
            if(!(myTools.getNString(sk,0,1)).equals("K"))return null;
            //用当前的skateID获取下一个点的经纬度，也就是当前点的经纬度
            Location l=LocationOP.UseSKgetLL(sk, roadKDataList,distanceDiff);
            assert l != null;
            data.setLatitude(l.getLatitude());
            data.setLongitude(l.getLongitude());
            data.setStakeId(l.getLocation());
        }
//        mark:孝汉应==89，别的的话还得改
        double carangle=89;
        data.setCarAngle(carangle);
        PathPoint p=new PathPoint(data.getDirection(), data.getId(),data.getLaneNo(),(int) newTpointno, data.getPlateNo(), data.getSpeed(),data.getTimeStamp(), data.getPlateColor(), data.getVehicleType(),data.getLongitude(),data.getLatitude(),data.getCarAngle(),data.getStakeId(),  data.getOriginalType(), data.getPlateColor());
        updateOnePointMap(p);
        myTools.printmergePoint(p);
        return p;
    }
    private static PathPoint predicOne(PathPointData PathPointData){
        return null;
    }
    //防撞，如果快碰到前车，则将其speedwindow全部-10
//    private static void deSpeedWindow(PathPointData PathPointData){
//        LinkedList<Float> speedWindow = PathPointData.getSpeedWindow();
//        ListIterator<Float> iterator = speedWindow.listIterator();
//        while (iterator.hasNext()) {
//            float originalValue = iterator.next();
//            iterator.set(originalValue - 10);
//        }
//    }
    private static float calculateMovingAverage(LinkedList<Float> speedWindow) {
            return (float) speedWindow.stream()
                    .mapToDouble(Float::doubleValue)
                    .average()
                    .orElse(Double.NaN);
    }
    private static void FirstEnterputNowMap(PathTData PathTData) throws IOException {
        List<PathPoint> p=PathTData.getPathList();
        for(PathPoint m:p){
            nowMap.put(m.getId(),new Pair<>(true,0));
        }
    }
    private static void updateMergePoint(PathTData PathTData) {
        for (PathPoint Point : PathTData.getPathList()) {//遍历当前这条TrajeData消息中的所有TrajePoint
            PathPointData data = PointMap.compute(Point.getId(), (k,v) -> new PathPointData());
            synchronized (data) {
                    data.getSpeedWindow().add(Point.getSpeed());
                    if (data.getSpeedWindow().size() > WINDOW_SIZE) {
                        data.getSpeedWindow().removeFirst();
                    }
                    data.setMileage(Point.getMileage());
                    data.setId(Point.getId());
                    data.setSpeed(Point.getSpeed());
                    data.setDirection(Point.getDirection());
                    data.setLatitude(Point.getLatitude());
                    data.setLongitude(Point.getLongitude());
                    data.setLaneNo(Point.getLaneNo());
                    data.setCarAngle(Point.getCarAngle());
                    data.setOriginalColor(Point.getOriginalColor());
                    data.setPlateColor(Point.getPlateColor());
                    data.setStakeId(Point.getStakeId());
                    data.setPlateNo(Point.getPlateNo());
                    data.setOriginalType(Point.getOriginalType());
                    data.setLastReceivedTime(PathTData.getTime());
                    data.setTimeStamp(Point.getTimeStamp());
                    data.setVehicleType(Point.getVehicleType());
                }
//            if(PointMap.get(Point.getId()).getMileage()==null)PointMap.remove(Point.getId());

        }
    }
    //没有更改lastreceivedtime，因为用在预测里，没有真的接受到
    private static void updateOnePointMap(PathPoint Point) {

                PathPointData data = PointMap.compute(Point.getId(), (k,v) -> new PathPointData());
            synchronized (data) {
                data.getSpeedWindow().add(Point.getSpeed());
                if (data.getSpeedWindow().size() > WINDOW_SIZE) {
                    data.getSpeedWindow().removeFirst();
                }
                data.setMileage(Point.getMileage());
                data.setId(Point.getId());
                data.setSpeed(Point.getSpeed());
                data.setDirection(Point.getDirection());
                data.setLatitude(Point.getLatitude());
                data.setLongitude(Point.getLongitude());
                data.setLaneNo(Point.getLaneNo());
                data.setCarAngle(Point.getCarAngle());
                data.setOriginalColor(Point.getOriginalColor());
                data.setPlateColor(Point.getPlateColor());
                data.setStakeId(Point.getStakeId());
                data.setPlateNo(Point.getPlateNo());
                data.setOriginalType(Point.getOriginalType());
                data.setTimeStamp(Point.getTimeStamp());
                data.setVehicleType(Point.getVehicleType());
            }
        if(PointMap.get(Point.getId()).getMileage()==null)PointMap.remove(Point.getId());

    }
    private static void updateOneJizhanPointMap(PathPoint Point) {

        PathPointData data = JizhanPointMap.compute(Point.getId(), (k,v) -> new PathPointData());
        synchronized (data) {
            data.getSpeedWindow().add(Point.getSpeed());
            if (data.getSpeedWindow().size() > WINDOW_SIZE) {
                data.getSpeedWindow().removeFirst();
            }
            data.setMileage(Point.getMileage());
            data.setId(Point.getId());
            data.setSpeed(Point.getSpeed());
            data.setDirection(Point.getDirection());
            data.setLatitude(Point.getLatitude());
            data.setLongitude(Point.getLongitude());
            data.setLaneNo(Point.getLaneNo());
            data.setCarAngle(Point.getCarAngle());
            data.setOriginalColor(Point.getOriginalColor());
            data.setPlateColor(Point.getPlateColor());
            data.setStakeId(Point.getStakeId());
            data.setPlateNo(Point.getPlateNo());
            data.setOriginalType(Point.getOriginalType());
            data.setTimeStamp(Point.getTimeStamp());
            data.setVehicleType(Point.getVehicleType());
        }
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
}
